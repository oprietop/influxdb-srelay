package relay

import (
	"github.com/influxdata/influxdb1-client/v2"
	"runtime"
	"strings"
	"time"
)

// increment the relay counters
func (h *HTTP) addCounters(l *loginfo) {
	// using a mutex, have to consider channels or sync.map
	h.mu.Lock()
	defer h.mu.Unlock()

	// Initialize the map value for each missing key
	k := l.traceroute
	if _, found := h.counters[k]; !found {
		h.counters[k] = &aggregation{}
	}

	// Increment the counters
	h.counters[k].Count += 1
	h.counters[k].BkDurationMs += l.bk_duration_ms
	h.counters[k].Duration += l.duration_ms
	h.counters[k].Latency += l.latency_ms
	h.counters[k].ReturnSize += l.returnsize
	h.counters[k].WritePoints += l.writepoints
	h.counters[k].WriteSize += l.writesize
	if l.status > 299 && l.status < 200 {
		h.counters[k].StatusNok += 1
	} else {
		h.counters[k].StatusOk += 1
	}

	// debug
	h.log.Debug().Msgf("Got %v urls in the counters map.", len(h.counters))
}

// return a point
func (h *HTTP) createPoint(m string, tags map[string]string, fields map[string]interface{}, ts time.Time) *client.Point {
	point, err := client.NewPoint(
		m,
		tags,
		fields,
		ts,
	)
	if err != nil {
		h.log.Error().Msgf("Error creating a influxdb point: ", err)
	}

	return point
}

// gather and return the counters as BatchPoints
func (h *HTTP) getPoints() client.BatchPoints {

	var T time.Time

	// Create the BatchPoints struct
	bp, err := client.NewBatchPoints(client.BatchPointsConfig{
		Database: h.cfg.MetricsDB,
	})
	if err != nil {
		h.log.Error().Msgf("Error creating the BatchPoints struct: ", err)
	}

	// Runtine Counters
	// Grab a snap of the mememory stats
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	tags := map[string]string{
		"Name":      h.cfg.Name,
		"GOVersion": runtime.Version(),
	}
	fields := map[string]interface{}{
		"NumCPU":       int64(runtime.NumCPU()),
		"NumGoroutine": int64(runtime.NumGoroutine()),
		"NumGC":        int64(m.NumGC),
		"TotalAlloc":   int64(m.TotalAlloc),
		"Alloc":        int64(m.Alloc),
		"Sys":          int64(m.Sys),
	}
	// create and add a point
	point := h.createPoint("runtime", tags, fields, T)
	bp.AddPoint(point)

	// Relay counters
	h.mu.Lock()
	defer h.mu.Unlock()
	// traverse the counters
	for k, _ := range h.counters {
		c := h.counters[k].Count
		tags := map[string]string{
			"Name": h.cfg.Name,
		}
		// Split the traceroute into tags
		items := strings.Split(k, "> ")
		for _, i := range items {
			pair := strings.Split(i, ":")
			if len(pair) == 2 {
				tags[pair[0]] = pair[1]
			}
		}
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
		// create and add a point
		point := h.createPoint("counters", tags, fields, T)
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
	defer c.Close()

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
