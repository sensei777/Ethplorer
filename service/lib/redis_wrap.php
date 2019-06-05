<?php

require_once __DIR__ . '/metric.php';

class RedisWrap {

    /**
     * @var Redis
     */
    protected $redis;

    public function __construct($redisInstance) {
        $this->redis = $redisInstance;
    }

    public function get($key) {
        Metrics::startRedisTiming('get', $key);
        $res = $this->redis->get($key);
        Metrics::writeRedisTiming('get', $key, strlen($res));
        return $res;
    }

    public function del($key) {
        Metrics::startRedisTiming('del', $key);
        $res = $this->redis->del($key);
        Metrics::writeRedisTiming('del', $key);
        return $res;
    }

    public function __call($name, $arguments)
    {
        if ($name === 'set') {
            $size = strlen($arguments[1]);
            Metrics::startRedisTiming('set', $arguments[0]);
            $res = call_user_func_array([$this->redis, $name], $arguments);
            Metrics::writeRedisTiming('set', $arguments[0], $size);
            return $res;
        }
        return call_user_func_array([$this->redis, $name], $arguments);
    }
}