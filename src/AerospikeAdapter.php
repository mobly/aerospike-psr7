<?php

namespace Mobly\Cache\Adapter\Aerospike;

use Mobly\Cache\AbstractCacheAdapter;
use Mobly\Cache\CacheAdapterConfiguration;
use Mobly\Cache\CacheItem;
use Mobly\Cache\Exception\CacheException;
use Psr\Cache\CacheItemInterface;

class AerospikeAdapter extends AbstractCacheAdapter
{

    const AEROSPIKE_NAMESPACE = 'namespace';

    const AEROSPIKE_SET = 'set';

    const AEROSPIKE_BIN = 'bin';

    /**
     * @type \Aerospike
     */
    private $cache;

    /**
     * @var self The reference to *Singleton* instance of this class
     */
    private static $instance;

    /**
     * @var CacheAdapterConfiguration
     */
    private $configuration;

    /**
     * @param CacheAdapterConfiguration $configuration
     */
    protected function __construct(CacheAdapterConfiguration $configuration)
    {
        $this->configuration = $configuration;

        $connectionConfig = $this->buildConnectionConfig();

        $this->cache = new \AeroSpike($connectionConfig, $this->configuration->getPersistent());
        $this->checkConnection();
    }

    /**
     * Private clone method to prevent cloning of the instance of the
     * *Singleton* instance.
     *
     * @return void
     */
    private function __clone()
    {
    }

    /**
     * @param CacheAdapterConfiguration $configuration
     * @return AerospikeAdapter
     */
    public static function getInstance(CacheAdapterConfiguration $configuration)
    {
        if (null === static::$instance) {
            static::$instance = new static($configuration);
        }

        return static::$instance;
    }

    /**
     * @return bool
     */
    protected function checkConnection()
    {
        if (!$this->cache->isConnected()) {
            throw new CacheException('Connection error!');
        }

        return true;
    }

    /**
     * @param CacheAdapterConfiguration $configuration
     */
    public function setConfiguration(CacheAdapterConfiguration $configuration)
    {
        $this->configuration = $configuration;
    }

    /**
     * @return array
     */
    private function buildConnectionConfig()
    {
        return [
            "hosts" => [
                ["addr" => $this->configuration->getHost(), "port" => $this->configuration->getPort()]
            ]
        ];
    }

    /**
     * @param $key
     * @return array
     */
    protected function transformKey($key)
    {
        return $this->cache->initKey(self::AEROSPIKE_NAMESPACE, self::AEROSPIKE_SET, $key);
    }

    /**
     * @param string $key
     * @return CacheItemInterface
     */
    protected function fetchObjectFromCache($key)
    {
        $cacheItem = new CacheItem($key);

        $transformedKey = $this->transformKey($key);

        $status = $this->cache->get($transformedKey, $record);
        if ($status == \Aerospike::OK) {
            $cacheItem->set($record['bins'][self::AEROSPIKE_BIN ]);
        }

        return $cacheItem;
    }

    /**
     * @param array $keys
     * @return array CacheItemInterface
     */
    protected function fetchMultiObjectsFromCache(array $keys)
    {
        $items = [];
        $initKeys = [];

        foreach ($keys as $key) {
            $initKeys[] = $this->transformKey($key);
        }

        $status = $this->cache->getMany($initKeys, $records);
        if ($status == \Aerospike::OK) {
            foreach ($records as $record) {
                $key = $record['key']['key'];
                $cacheItem = new CacheItem($key);
                if ($record['bins'][self::AEROSPIKE_BIN ] !== null) {
                    $cacheItem->set($record['bins'][self::AEROSPIKE_BIN ]);
                }
                $items[$key] = $cacheItem;
            }
        }

        return $items;
    }

    /**
     * @param CacheItemInterface $cacheItem
     * @param $key
     */
    protected function getObjectByKey(CacheItemInterface $cacheItem, $key)
    {
        $transformedKey = $this->transformKey($key);

        $status = $this->cache->get($transformedKey, $record);
        if ($status == \Aerospike::OK) {
            $cacheItem->set($record['bins'][self::AEROSPIKE_BIN ]);
        }
    }

    protected function clearAllObjectsFromCache()
    {
        $this->cache->scan(self::AEROSPIKE_NAMESPACE, self::AEROSPIKE_SET, function ($record) {
            if (isset($record['key']['key'])) {
                $this->deleteItem($record['key']['key']);
            }
        });
    }

    /**
     * @param string $key
     * @return bool
     */
    protected function clearOneObjectFromCache($key)
    {
        $key = $this->transformKey($key);
        $this->commit();

        $status = $this->cache->remove($key);
        if ($status == \Aerospike::OK) {
            return true;
        }

        return false;
    }

    /**
     * @param string $key
     * @param CacheItemInterface $item
     * @param int|null $ttl
     * @return bool
     */
    protected function storeItemInCache($key, CacheItemInterface $item, $ttl)
    {
        if ($ttl === null) {
            $ttl = $this->configuration->getTimeToLive();
        }

        // always save the key in record
        $policy = [\Aerospike::OPT_POLICY_KEY => \Aerospike::POLICY_KEY_SEND];

        $transformedKey = $this->transformKey($key);

        $bins = [self::AEROSPIKE_BIN => $item->get()];
        $status = $this->cache->put($transformedKey, $bins, $ttl, $policy);
        if ($status == \Aerospike::OK) {
            return true;
        }

        return false;
    }

}