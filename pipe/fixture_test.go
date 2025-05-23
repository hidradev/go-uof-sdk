package pipe

import (
	"sync"
	"testing"
	"time"

	"github.com/hidradev/go-uof-sdk"
	"github.com/stretchr/testify/assert"
)

type fixtureAPIMock struct {
	preloadTo time.Time
	eventURN  uof.URN
	//requests map[int]struct{}
	sync.Mutex
}

func (m *fixtureAPIMock) Fixture(lang uof.Lang, eventURN uof.URN) (uof.Fixture, error) {
	m.eventURN = eventURN
	return uof.Fixture{}, nil
}

func (m *fixtureAPIMock) Fixtures(lang uof.Lang, to time.Time) (<-chan uof.Fixture, <-chan error) {
	m.preloadTo = to
	out := make(chan uof.Fixture)
	errc := make(chan error)
	go func() {
		close(out)
		close(errc)
	}()
	return out, errc
}

func (m *fixtureAPIMock) FixtureBytes(lang uof.Lang, eventURN uof.URN) ([]byte, error) {
	m.eventURN = eventURN
	return nil, nil
}

func (m *fixtureAPIMock) DailySchedule(lang uof.Lang, date string) ([]uof.Fixture, error) {
	return nil, nil
}

func TestFixturePipe(t *testing.T) {
	a := &fixtureAPIMock{}
	f := Fixture(a, []uof.Lang{uof.LangEN, uof.LangDE}, 3)
	assert.NotNil(t, f)

	in := make(chan *uof.Message)
	out, _ := f(in)

	// this type of message is passing through
	m := uof.NewSimpleConnnectionMessage(uof.ConnectionStatusUp)
	in <- m
	om := <-out
	assert.Equal(t, m, om)

	m = fixtureChangeMsg(t)
	in <- m
	om = <-out
	assert.Equal(t, m, om)

	close(in)
	for range out {
	}

	assert.Equal(t, a.eventURN, m.FixtureChange.EventURN)
}

func fixtureChangeMsg(t *testing.T) *uof.Message {
	buf := []byte(`<fixture_change event_id="sr:match:1234" product="3" start_time="1511107200000"/>`)
	m, err := uof.NewQueueMessage("hi.pre.-.fixture_change.1.sr:match.1234.-", buf)
	assert.NoError(t, err)
	return m
}
