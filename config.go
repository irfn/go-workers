package wrkrs

import (
	"strconv"
	"time"

	"github.com/garyburd/redigo/redis"
)

type config struct {
	processId    string
	Namespace    string
	PollInterval int
	Pool         *redis.Pool
	Fetch        func(queue string) Fetcher
	DialTimeout  int
	ReadTimeout  int
	WriteTimeout int
}

var Config *config

func Configure(options map[string]string) {
	var poolSize int
	var namespace string
	var pollInterval int
	var dialTimeout int
	var readTimeout int
	var writeTimeout int

	if options["server"] == "" {
		panic("Configure requires a 'server' option, which identifies a Redis instance")
	}
	if options["process"] == "" {
		panic("Configure requires a 'process' option, which uniquely identifies this instance")
	}
	if options["pool"] == "" {
		options["pool"] = "1"
	}
	if options["namespace"] != "" {
		namespace = options["namespace"] + ":"
	}
	if seconds, err := strconv.Atoi(options["poll_interval"]); err == nil {
		pollInterval = seconds
	} else {
		pollInterval = 15
	}
	if milliseconds, err := strconv.Atoi(options["dial_timeout"]); err == nil {
		dialTimeout = milliseconds
	} else {
		dialTimeout = 500
	}
	if milliseconds, err := strconv.Atoi(options["read_timeout"]); err == nil {
		readTimeout = milliseconds
	} else {
		readTimeout = 500
	}
	if milliseconds, err := strconv.Atoi(options["write_timeout"]); err == nil {
		writeTimeout = milliseconds
	} else {
		writeTimeout = 500
	}

	poolSize, _ = strconv.Atoi(options["pool"])

	Config = &config{
		options["process"],
		namespace,
		pollInterval,
		&redis.Pool{
			MaxIdle:     poolSize,
			IdleTimeout: 240 * time.Second,
			Dial: func() (redis.Conn, error) {
				c, err := redis.Dial("tcp", options["server"],
					redis.DialConnectTimeout(time.Duration(dialTimeout)*time.Millisecond),
					redis.DialReadTimeout(time.Duration(readTimeout)*time.Millisecond),
					redis.DialWriteTimeout(time.Duration(writeTimeout)*time.Millisecond),
				)
				if err != nil {
					return nil, err
				}
				if options["password"] != "" {
					if _, err := c.Do("AUTH", options["password"]); err != nil {
						c.Close()
						return nil, err
					}
				}
				if options["database"] != "" {
					if _, err := c.Do("SELECT", options["database"]); err != nil {
						c.Close()
						return nil, err
					}
				}
				return c, err
			},
			TestOnBorrow: func(c redis.Conn, t time.Time) error {
				_, err := c.Do("PING")
				return err
			},
		},
		func(queue string) Fetcher {
			return NewFetch(queue, make(chan *Msg), make(chan bool))
		},
		dialTimeout,
		readTimeout,
		writeTimeout,
	}
}
