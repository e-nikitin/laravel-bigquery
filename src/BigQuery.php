<?php

namespace nikitin\BigQuery;

use Google\Cloud\BigQuery\BigQueryClient;
use Illuminate\Support\Facades\Cache;
use Madewithlove\IlluminatePsrCacheBridge\Laravel\CacheItemPool;
use Illuminate\Support\Arr;

/**
 * Class BigQuery
 * @package nikitin\BigQuery
 */
class BigQuery
{

    /**
     * @param string|null $project_id
     * @return BigQueryClient
     */
    public function makeClient(string $project_id = null)
    {
        $bigQueryConfig = config('bigquery');
        $project_id = empty($project_id) ? $bigQueryConfig['project_id'] : $project_id;
        
        $clientConfig = array_merge([
            'projectId' => $project_id,
            'keyFilePath' => $bigQueryConfig['application_credentials'],
            'authCache' => $this->configureCache($bigQueryConfig['auth_cache_store']),
        ], Arr::get($bigQueryConfig, 'client_options', []));

        return new BigQueryClient($clientConfig);
    }

    /**
     * @param $cacheStore
     * @return CacheItemPool
     */
    protected function configureCache($cacheStore)
    {
        $store = Cache::store($cacheStore);
        $cache = new CacheItemPool($store);
        return $cache;
    }
}