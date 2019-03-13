<?php
namespace nikitin\BigQuery\Facades;

use Illuminate\Support\Facades\Facade;

/**
 * Class BigQuery
 * @package nikitin\BigQuery\Facades
 * 
 * @method static \Google\Cloud\BigQuery\BigQueryClient makeClient($project_id = null)
 */
class BigQuery extends Facade
{
    protected static function getFacadeAccessor()
    {
        return \nikitin\BigQuery\BigQuery::class;
    }
}