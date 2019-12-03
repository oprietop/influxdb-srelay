package relay

import (
	"github.com/influxdata/influxdb1-client/v2"
	"time"
)

// increment the relay counters
func (h *HTTP) addCounters(l loginfo) {
	// using a mutex, have to consider channels or sync.map
	h.mu.Lock()
	defer h.mu.Unlock()

	// Create initialize the map value for each missing key
	u := l.url
	if _, found := h.counters[u]; !found {
		h.counters[u] = &aggregation{}
	}

	// Increment the counters
	h.counters[u].Count += 1
	h.counters[u].BkDurationMs += l.bk_duration_ms
	h.counters[u].Duration += l.duration_ms
	h.counters[u].Latency += l.latency_ms
	h.counters[u].ReturnSize += l.returnsize
	h.counters[u].WritePoints += l.writepoints
	h.counters[u].WriteSize += l.writesize
	if l.status > 299 && l.status < 200 {
		h.counters[u].StatusNok += 1
	} else {
		h.counters[u].StatusOk += 1
	}

	// debug
	h.log.Debug().Msgf("Got %v urls in the counters map.", len(h.counters))
}

// reaturn the average counters as BatchPoints
func (h *HTTP) getPoints() client.BatchPoints {
	h.mu.Lock()
	defer h.mu.Unlock()

	var T time.Time

	// Create the BatchPoints struct
	bp, err := client.NewBatchPoints(client.BatchPointsConfig{
		Database: h.cfg.MetricsDB,
	})
	if err != nil {
		h.log.Error().Msgf("Error creating the BatchPoints struct: ", err)
	}

	// traverse the counters
	for k, _ := range h.counters {
		c := h.counters[k].Count
		tags := map[string]string{"url": k}
		fields := map[string]interface{}{
			"AvgBkDuration":  h.counters[k].BkDurationMs.Microseconds() / int64(c),
			"AvgDuration":    h.counters[k].Duration.Microseconds() / int64(c),
			"AvgLatency":     h.counters[k].Latency.Microseconds() / int64(c),
			"AvgReturnSize":  h.counters[k].ReturnSize / c,
			"AvgWritePoints": h.counters[k].WritePoints / c,
			"AvgWriteSize":   h.counters[k].WriteSize / c,
			"StatusNok":      h.counters[k].StatusNok,
			"StatusOk":       h.counters[k].StatusOk,
			"Count":          h.counters[k].Count,
		}
		// create a point
		point, err := client.NewPoint(
			"counters",
			tags,
			fields,
			T,
		)
		if err != nil {
			h.log.Error().Msgf("Error creating a influxdb point: ", err)
		}
		// append the points
		bp.AddPoint(point)
		// clean the counters for that key
		delete(h.counters, k)
	}
	h.log.Debug().Msgf("Got %v influxdb points.", len(bp.Points()))

	return bp
}

// Send the runtime relay counters to influxdb
func (h *HTTP) sendCounters() {
	// Create a client
	c, err := client.NewHTTPClient(client.HTTPConfig{
		Addr:     h.cfg.MetricsHost,
		Username: h.cfg.MetricsUser,
		Password: h.cfg.MetricsPass,
	})
	if err != nil {
		h.log.Error().Msgf("Error creating the influxdb client: ", err)
	}

	// get the points
	bp := h.getPoints()
	// fire the points
	num := len(bp.Points())
	if num > 0 {
		h.log.Info().Msgf("Sending %v points to influxdb.", num)
		err = c.Write(bp)
	}
	if err != nil {
		h.log.Error().Msgf("Error sending points to influxdb: ", err)
	}
}
