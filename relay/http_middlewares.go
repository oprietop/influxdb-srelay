package relay

import (
	"compress/gzip"

	"net/http"

	"time"

	"github.com/toni-moreno/influxdb-srelay/relayctx"
	"github.com/toni-moreno/influxdb-srelay/utils"
)

// Struct to hold the logging items
type loginfo struct {
	traceroute     string
	referer        string
	url            string
	writesize      int
	writepoints    int
	returnsize     int
	duration_ms    time.Duration
	bk_duration_ms time.Duration
	latency_ms     time.Duration
	status         int
	method         string
	user           string
	source         string
	xff            string
	useragent      string
}

func allMiddlewares(h *HTTP, handlerFunc relayHandlerFunc) relayHandlerFunc {
	var res = handlerFunc
	for _, middleware := range middlewares {
		res = middleware(h, res)
	}
	return res
}

func (h *HTTP) bodyMiddleWare(next relayHandlerFunc) relayHandlerFunc {
	return relayHandlerFunc(func(h *HTTP, w http.ResponseWriter, r *http.Request) {
		h.log.Debug().Msg("----------------------INIT bodyMiddleWare------------------------")
		var body = r.Body
		if r.Header.Get("Content-Encoding") == "gzip" {
			b, err := gzip.NewReader(r.Body)
			if err != nil {
				relayctx.JsonResponse(w, r, http.StatusBadRequest, "unable to decode gzip body")
				return
			}
			defer b.Close()
			body = b
		}

		r.Body = body
		next(h, w, r)
		h.log.Debug().Msg("----------------------END bodyMiddleWare------------------------")
	})
}

func (h *HTTP) queryMiddleWare(next relayHandlerFunc) relayHandlerFunc {
	return relayHandlerFunc(func(h *HTTP, w http.ResponseWriter, r *http.Request) {
		h.log.Debug().Msg("----------------------INIT queryMiddleWare------------------------")
		queryParams := r.URL.Query()

		if queryParams.Get("db") == "" && (r.URL.Path == "/write" || r.URL.Path == "/api/v1/prom/write") {
			relayctx.JsonResponse(w, r, http.StatusBadRequest, "missing parameter: db")
			return
		}

		if queryParams.Get("rp") == "" && h.rp != "" {
			queryParams.Set("rp", h.rp)
		}

		r.URL.RawQuery = queryParams.Encode()
		next(h, w, r)
		h.log.Debug().Msg("----------------------END queryMiddleWare------------------------")
	})

}

func (h *HTTP) logMiddleWare(next relayHandlerFunc) relayHandlerFunc {
	return relayHandlerFunc(func(h *HTTP, w http.ResponseWriter, r *http.Request) {
		h.log.Debug().Msg("----------------------INIT logMiddleWare------------------------")
		next(h, w, r)
		rc := relayctx.GetRelayContext(r)
		if rc.Served {
			// populape the loginfo struct
			l := loginfo{
				traceroute:     rc.TraceRoute.String(),
				referer:        r.Referer(),
				url:            r.URL.String(),
				writesize:      rc.RequestSize,
				writepoints:    rc.RequestPoints,
				returnsize:     rc.SentDataLength,
				duration_ms:    time.Since(rc.InputTime),
				bk_duration_ms: time.Since(rc.BackendTime),
				latency_ms:     rc.BackendTime.Sub(rc.InputTime),
				status:         rc.SentHTTPStatus,
				method:         r.Method,
				user:           utils.GetUserFromRequest(r),
				source:         r.RemoteAddr,
				xff:            r.Header.Get("X-Forwarded-For"),
				useragent:      r.UserAgent(),
			}
			// Send an access log entry
			h.acclog.Info().
				Str("trace-route", l.traceroute).
				Str("referer", l.referer).
				Str("url", l.url).
				Int("write-size", l.writesize).
				Int("write-points", l.writepoints).
				Int("returnsize", l.returnsize).
				Dur("duration_ms", l.duration_ms).
				Dur("bk_duration_ms", l.bk_duration_ms).
				Dur("latency_ms", l.latency_ms).
				Int("status", l.status).
				Str("method", l.method).
				Str("user", l.user). // <---allready computed from http_params !! REVIEW!!!
				Str("source", l.source).
				Str("xff", l.xff).
				Str("user-agent", l.useragent).
				Msg("")
			// Increment our metrics counters
			h.addCounters(&l)
		}
		h.log.Debug().Msg("----------------------END logMiddleWare------------------------")
	})
}

func (h *HTTP) rateMiddleware(next relayHandlerFunc) relayHandlerFunc {
	return relayHandlerFunc(func(h *HTTP, w http.ResponseWriter, r *http.Request) {
		h.log.Debug().Msg("----------------------INIT rateMiddleware-----------------------")
		if h.rateLimiter != nil && !h.rateLimiter.Allow() {
			h.log.Debug().Msgf("Rate Limited => Too Many Request (Limit %+v)(Burst %d) ", h.rateLimiter.Limit(), h.rateLimiter.Burst)
			relayctx.JsonResponse(w, r, http.StatusTooManyRequests, http.StatusText(http.StatusTooManyRequests))
			return
		}

		next(h, w, r)
		h.log.Debug().Msg("----------------------END rateMiddleware-----------------------")
	})
}
