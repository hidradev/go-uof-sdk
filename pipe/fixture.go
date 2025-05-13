package pipe

import (
	"sync"
	"time"

	"github.com/hidradev/go-uof-sdk"
)

type fixtureAPI interface {
	FixtureBytes(lang uof.Lang, eventURN uof.URN) ([]byte, error)
	Fixture(lang uof.Lang, eventURN uof.URN) (uof.Fixture, error)
	Fixtures(lang uof.Lang, to time.Time) (<-chan uof.Fixture, <-chan error)
	DailySchedule(lang uof.Lang, date string) ([]uof.Fixture, error)
}

type fixture struct {
	api       fixtureAPI
	languages []uof.Lang // suported languages
	em        *expireMap
	errc      chan<- error
	out       chan<- *uof.Message
	preloadTo time.Time
	subProcs  *sync.WaitGroup

	fetchInterval time.Duration
	prefetchDay   int

	rateLimit chan struct{}
	sync.Mutex
}

func Fixture(api fixtureAPI, languages []uof.Lang, preloadTo time.Time, fetchLooping bool) InnerStage {
	f := &fixture{
		api:       api,
		languages: languages,
		em:        newExpireMap(time.Hour * 12),
		//requests:  make(map[string]time.Time),
		subProcs:  &sync.WaitGroup{},
		rateLimit: make(chan struct{}, ConcurrentAPICallsLimit),
		preloadTo: preloadTo,
		// 1d
		fetchInterval: time.Hour * 24,
		prefetchDay:   3,
	}

	if fetchLooping {
		// looping on 1d interval (00:00:00)
		go func() {
			for {
				now := time.Now()
				next := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, now.Location())
				if now.After(next) {
					next = next.AddDate(0, 0, 1)
				}

				// wait until next day
				time.Sleep(next.Sub(now))

				f.fetchFixtures()
			}
		}()
	}

	return StageWithSubProcessesSync(f.loop)
}

// 주기적으로 현재 시점 기준 +3일 이벤트를 가져오는 메소드
func (f *fixture) fetchFixtures() {
	day := time.Now().AddDate(0, 0, f.prefetchDay)

	for _, lang := range f.languages {
		fixtures, errc := f.api.Fixtures(lang, day)

		go func() {
			for err := range errc {
				f.errc <- err
			}
		}()

		for fixture := range fixtures {
			f.em.insert(uof.UIDWithLang(fixture.URN.EventID(), lang))
		}
	}
}

// 여기서 주의해야 할 사항:
//   - 시작 시 preload를 수행합니다.
//   - preload 중에는 개별적으로 실행하지 않습니다.
//   - preload 중에는 체인을 중단하지 않고, in에서 out으로 계속 전송합니다.
//   - preload가 완료된 후 preload에 포함되지 않은 항목을 만듭니다.
//   - x초보다 자주 요청하지 않습니다 (재생 시 많은 요청을 생성하지 않도록 중요합니다).
//   - 시나리오 재생 시, 일부가 처리 중일 때는 동일한 것을 시작하지 않고 한 번만 실행되기를 원합니다.
func (f *fixture) loop(in <-chan *uof.Message, out chan<- *uof.Message, errc chan<- error) *sync.WaitGroup {
	f.errc, f.out = errc, out

	for _, u := range f.preloadLoop(in) {
		f.getFixture(u, uof.CurrentTimestamp(), true)
	}
	for m := range in {
		if u := f.eventURN(m); u != uof.NoURN {
			f.getFixture(u, m.ReceivedAt, false)
		}
		out <- m
	}

	return f.subProcs
}

func (f *fixture) eventURN(m *uof.Message) uof.URN {
	if m.Type == uof.MessageTypeOddsChange && m.OddsChange != nil {
		return m.OddsChange.EventURN
	}
	if m.Type == uof.MessageTypeBetSettlement && m.BetSettlement != nil {
		return m.BetSettlement.EventURN
	}
	if m.Type != uof.MessageTypeFixtureChange || m.FixtureChange == nil {
		return uof.NoURN
	}
	return m.FixtureChange.EventURN
}

// returns list of fixture changes urns appeared in 'in' during preload
func (f *fixture) preloadLoop(in <-chan *uof.Message) []uof.URN {
	done := make(chan struct{})

	f.subProcs.Add(1)
	func() {
		defer f.subProcs.Done()
		f.preload()
		close(done)
	}()

	var urns []uof.URN
	for {
		select {
		case m, ok := <-in:
			if !ok {
				return urns
			}
			f.out <- m
			if u := f.eventURN(m); u != uof.NoURN {
				urns = append(urns, u)
			}
		case <-done:
			return urns
		}
	}
}

func (f *fixture) preload() {
	if f.preloadTo.IsZero() {
		return
	}
	var wg sync.WaitGroup
	wg.Add(len(f.languages))
	for _, lang := range f.languages {
		func(lang uof.Lang) {
			defer wg.Done()
			in, errc := f.api.Fixtures(lang, f.preloadTo)
			for x := range in {
				f.out <- uof.NewFixtureMessage(lang, x, uof.CurrentTimestamp())
				f.em.insert(uof.UIDWithLang(x.URN.EventID(), lang))
			}
			for err := range errc {
				f.errc <- err
			}
		}(lang)
	}
	wg.Wait()
}

func (f *fixture) getFixture(eventURN uof.URN, receivedAt int, isPreload bool) {
	f.subProcs.Add(len(f.languages))
	for _, lang := range f.languages {
		func(lang uof.Lang) {
			defer f.subProcs.Done()
			f.rateLimit <- struct{}{}
			defer func() { <-f.rateLimit }()

			key := uof.UIDWithLang(eventURN.EventID(), lang)
			if isPreload && f.em.fresh(key) {
				return
			}
			buf, err := f.api.FixtureBytes(lang, eventURN)
			if err != nil {
				f.errc <- err
				return
			}

			m, err := uof.NewFixtureMessageFromBuf(lang, buf, receivedAt)
			if err != nil {
				f.errc <- err
				return
			}
			f.out <- m
			f.em.insert(key)
		}(lang)
	}
}
