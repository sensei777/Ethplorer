<?php

class Metrics {

    const STATSD_CACHE_PREFIX = 'cache';

    const STATSD_API_METHOD_PREFIX = 'api-method';

    /**
     * @var bool|\Domnikl\Statsd\Client
     */
    static protected $metric = FALSE;

    private function __construct() {}

    /**
     * @var array
     */
    static protected $cachePrefixesWithDash = [
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

    static public function initMetric(array $statsd) {
        static::$metric = new \Domnikl\Statsd\Client(
            new \Domnikl\Statsd\Connection\UdpSocket($statsd['host'], $statsd['port']),
            $statsd['prefix']
        );
    }

    static protected function getCachePrefix($method, $cacheKey) {
        foreach (static::$cachePrefixesWithDash as $prefix) {
            if (strpos($cacheKey, $prefix) === 0) {
                return $prefix;
            }
        }
        return explode('-', $cacheKey)[0];
    }

    static public function startCacheTiming($method, $cacheKey) {
        if (static::$metric) {
            $cachePrefix = static::getCachePrefix($method, $cacheKey);
            static::$metric->startTiming(sprintf('%s.%s.%s', self::STATSD_CACHE_PREFIX, $method, $cachePrefix));
        }
    }

    static public function writeCacheTiming($method, $cacheKey, $size = null) {
        if (static::$metric) {
            $cachePrefix = static::getCachePrefix($method, $cacheKey);
            static::$metric->endTiming(sprintf('%s.%s.%s', self::STATSD_CACHE_PREFIX, $method, $cachePrefix));
            if ($size) {
                static::$metric->timing(sprintf('%s.%s.size.%s', self::STATSD_CACHE_PREFIX, $method, $cacheKey), $size);
            }
        }
    }

    static public function writeApiMethodTiming($method, $time, $memUsage, $peakMemUsage) {
        if (static::$metric) {
            static::$metric->timing(sprintf('%s.%s', self::STATSD_API_METHOD_PREFIX, $method), $time);
            static::$metric->timing(sprintf('%s.mem.%s', self::STATSD_API_METHOD_PREFIX, $method), $memUsage);
            static::$metric->timing(sprintf('%s.mem-peak.%s', self::STATSD_API_METHOD_PREFIX, $method), $peakMemUsage);
        }
    }
}
