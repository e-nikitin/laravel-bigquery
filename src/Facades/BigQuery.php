<?php
namespace nikitin\BigQuery\Facades;

use Illuminate\Support\Facades\Facade;

/**
 * Class BigQuery
 * @package nikitin\BigQuery\Facades
 *
 * @method static \Google\Cloud\BigQuery\BigQueryClient makeClient($project_id = null)
 * @method static void prepareData(\Illuminate\Support\Collection $data)
 * @method static bool truncate(string $dataset, string $table, string $project_id = null)
 */
class BigQuery extends Facade
{
    protected static function getFacadeAccessor()
    {
        return \nikitin\BigQuery\BigQuery::class;
    }
}
