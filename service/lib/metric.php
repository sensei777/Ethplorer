<?php

class Metrics {

    const STATSD_REDIS_PREFIX = 'redis';

    const STATSD_API_METHOD_PREFIX = 'api-method';

    /**
     * @var bool|\Domnikl\Statsd\Client
     */
    static protected $metric = false;

    static protected $timings = [];

    static protected $startTimings = [];

    static protected $apiMethodName = '-';

    static protected $statsd = false;

    static protected $statsdOptions = false;

    private function __construct() {}

    /**
     * @var array
     */
    static protected $redisPrefixesWithDash = [
        'highloaded-address',
        'top_tokens-by-period-volume',
        'top_tokens-by-current-volume',
        'block-txs',
        'rates-history',
        'cap-history',
        'tokens',
        'lastBlock',
        'top_tokens_totals',
        'tokens-simple'
    ];

    static public function setApiMethodName($name) {
        self::$apiMethodName = $name;
    }

    static protected function setTiming($prefix, $value) {
        if(empty(self::$timings[$prefix])) {
            self::$timings[$prefix] = [];
        }
        self::$timings[$prefix][] = $value;
    }

    protected function startTiming($prefix) {
        self::$startTimings[$prefix] = microtime(true);
    }

    protected function stopTiming($prefix) {
        if (!empty(self::$startTimings[$prefix])) {
            self::setTiming($prefix, microtime(true) - self::$startTimings[$prefix]);
            unset(self::$startTimings[$prefix]);
        }
    }

    static public function initMetric(array $statsdOptions) {
        self::$metric = true;
        self::$statsdOptions = $statsdOptions;
        self::$statsd = false;
        register_shutdown_function(function() {
            if (!(empty(self::$metric))) {
                $time = microtime(true);
                self::sendMetrics();
                $statsd = self::getConnections();
                if (!empty($statsd)) {
                    $statsd->timing('statsd.metric-send', microtime(true) - $time);
                }
            }
        });
    }

    static protected function getConnections() {
        if (empty(self::$statsd) && !empty(self::$statsdOptions)) {
            $time = microtime(true);
            $connection = new \Domnikl\Statsd\Connection\UdpSocket(
                self::$statsdOptions['host'],
                self::$statsdOptions['port'],
                isset(self::$statsdOptions['timeout']) ? self::$statsdOptions['timeout'] : 0.5,
                isset(self::$statsdOptions['persist']) ? self::$statsdOptions['persist'] : true
            );
            self::$statsd = new \Domnikl\Statsd\Client($connection, self::$statsdOptions['prefix']);
            self::setTiming('statsd.connection-time', microtime(true) - $time);
        }
        return self::$statsd;
    }

    static public function sendMetrics() {
        if (!(empty(self::$metric))) {
            $timings = self::$timings;
            $statsd = self::getConnections();
            if (empty($statsd)) {
                return;
            }
            $statsd->startBatch();
            foreach ($timings as $prefix => $metricValues) {
                foreach ($metricValues as $value) {
                    $statsd->timing($prefix, $value);
                }
            }
            self::$timings = [];
            $statsd->endBatch();
        }
    }

    static protected function getRedisKeyPrefix($redisKey) {
        foreach (self::$redisPrefixesWithDash as $prefix) {
            if (strpos($redisKey, $prefix) === 0) {
                return $prefix;
            }
        }
        return explode('-', $redisKey)[0];
    }

    static public function startRedisTiming($method, $redisKey) {
        if (self::$metric) {
            $redisKeyPrefix = self::getRedisKeyPrefix($redisKey);
            self::startTiming(sprintf(
                '%s.%s.times.%s',
                self::STATSD_REDIS_PREFIX,
                $method,
                $redisKeyPrefix
            ));
        }
    }

    static public function writeRedisTiming($method, $redisKey, $size = null) {
        if (self::$metric) {
            $redisKeyPrefix = self::getRedisKeyPrefix($redisKey);
            self::stopTiming(
                sprintf(
                    '%s.%s.times.%s',
                    self::STATSD_REDIS_PREFIX,
                    $method,
                    $redisKeyPrefix
                ));
            if ($size) {
                self::setTiming(
                    sprintf(
                        '%s.%s.size.%s',
                        self::STATSD_REDIS_PREFIX,
                        $method,
                        $redisKeyPrefix
                    ), $size);
            }
        }
    }

    static public function writeApiMethodTiming($method, $time, $memUsage, $peakMemUsage) {
        if (self::$metric) {
            self::setTiming(sprintf('%s.times.%s', self::STATSD_API_METHOD_PREFIX, $method), $time);
            self::setTiming(sprintf('%s.mem.%s', self::STATSD_API_METHOD_PREFIX, $method), $memUsage);
            self::setTiming(sprintf('%s.mem-peak.%s', self::STATSD_API_METHOD_PREFIX, $method), $peakMemUsage);
        }
    }
}
