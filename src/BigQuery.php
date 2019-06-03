<?php

namespace nikitin\BigQuery;

use App\Helpers\ExceptionNotificator\ExceptionNotificator;
use Google\Cloud\BigQuery\BigQueryClient;
use Google\Cloud\BigQuery\QueryJobConfiguration;
use Google\Cloud\Core\ExponentialBackoff;
use Illuminate\Support\Facades\Log;
use Illuminate\Support\Facades\Storage;
use Illuminate\Support\Collection;
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

        $store = Cache::store($bigQueryConfig['auth_cache_store']);
        $cache = new CacheItemPool($store);

        $clientConfig = array_merge([
            'projectId' => $project_id,
            'keyFilePath' => $bigQueryConfig['application_credentials'],
            'authCache' => $cache,
        ], Arr::get($bigQueryConfig, 'client_options', []));

        return new BigQueryClient($clientConfig);
    }


    /**
     * @param Collection $data
     */
    public function prepareData(Collection $data): void
    {
        $data->transform(function ($item) {
            return [
                'data' => $item
            ];
        });
    }

    /**
     * @param string $dataset
     * @param string $table
     * @param string|null $project_id
     * @return bool
     */
    public function truncate(string $dataset, string $table, string $project_id = null)
    {
        $client = $this->makeClient($project_id);
        $query = $client->query("DELETE FROM $dataset.$table WHERE 1=1");
        return $this->runQuery($query, $client)->isComplete();
    }

    /**
     * @param array $data
     * @return array
     */
    public function handleSelectResult(array $data)
    {
        if (!Arr::get($data, 'rows', false))
            return [];

        $fields = collect($data['schema']['fields'])->map(function ($item) {
            return $item['name'];
        })->toArray();

        return collect($data['rows'])
            ->map(function ($item) use ($fields) {
                return collect($item['f'])->mapWithKeys(function ($item, $k) use ($fields) {
                    return [$fields[$k] => $item['v']];
                });
            })->toArray();
    }

    /**
     * @param QueryJobConfiguration $query
     * @param BigQueryClient $client
     * @param int $try
     * @return \Google\Cloud\BigQuery\QueryResults
     * @throws \Exception
     */
    public function runQuery(QueryJobConfiguration $query, BigQueryClient $client, int $try = 5)
    {
        try {
            $qr = $client->runQuery($query);
            $timer = 60;
            while ($timer <= 0) {
                if ($qr->isComplete()) {
                    return $qr;
                }
                sleep(1);
                $qr->reload();
                --$timer;
            }

            return $qr;
        } catch (\Exception $e) {
            if ($try <= 0 || $e->getCode() != 403)
                throw $e;
            sleep(config('bigquery.sleep_time_403', 10));
            return $this->runQuery($query, $client, --$try);
        }
    }

    /**
     * @param string $file
     * @param string $table
     * @param array $fields
     * @param string|null $dataset
     * @param null $project_id
     * @throws \Exception
     */
    public function saveFromFile(string $file, string $table, array $fields, string $dataset = null, $project_id = null)
    {
        $dataset = $dataset ?? env('GOOGLE_CLOUD_DATASET');
        $client = $this->makeClient($project_id);
        $tableInfo = $client->dataset($dataset)->table($table)->info();
        $tableFields = collect($tableInfo['schema']['fields']);

        $schema = [
            'fields' => collect($fields)->map(function ($k) use ($tableFields) {
                return $tableFields->first(function ($field) use ($k) {
                    return $field['name'] == $k;
                });
            })->values()->toArray()
        ];

        $tableBq = $client->dataset($dataset)->table($table);

        $loadConfig = $tableBq->load(fopen($file, 'r'))
            ->sourceFormat('CSV')
            ->fieldDelimiter(';')
            ->schema($schema);
        $job = $tableBq->runJob($loadConfig);

        $backoff = new ExponentialBackoff(10);

        $backoff->execute(function () use ($job) {
            //printf('Waiting for job to complete' . PHP_EOL);
            $job->reload();
            if (!$job->isComplete()) {
                throw new \Exception('Job has not yet completed', 500);
            }
        });

        if (isset($job->info()['status']['errorResult'])) {
            throw new \Exception('Error during saving to BQ' . PHP_EOL . print_r($job->info(), 1));
        }


    }

}
