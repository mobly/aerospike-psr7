<?php

namespace Mobly\Cache\Adapter\Aerospike;

use Mobly\Cache\CacheAdapterConfiguration;

/**
 * Class CacheAdapterConfiguration
 * @package Mobly\Cache
 */
class AerospikeConfiguration extends CacheAdapterConfiguration
{
    /**
     * @var string
     */
    protected $namespace;

    /**
     * @var string
     */
    protected $set;

    protected $required = [
        'host',
        'port',
        'namespace',
        'set'
    ];

    /**
     * @param $namespace
     */
    public function setNamespace($namespace)
    {
        $this->namespace = (string) $namespace;
    }

    /**
     * @return string
     */
    public function getNamespace()
    {
        return (string) $this->namespace;
    }

    /**
     * @param $set
     */
    public function setSet($set)
    {
        $this->set = (string) $set;
    }

    /**
     * @return string
     */
    public function getSet()
    {
        return (string) $this->set;
    }
}
