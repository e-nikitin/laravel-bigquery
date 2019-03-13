<?php

namespace nikitin\BigQuery;

use Illuminate\Support\ServiceProvider;
//use Illuminate\Contracts\Support\DeferrableProvider;
use nikitin\BigQuery\Exceptions\InvalidConfiguration;

class BigQueryServiceProvider extends ServiceProvider //implements DeferrableProvider
{
    protected $defer = true;

    /**
     * Perform post-registration booting of services.
     *
     * @return void
     */
    public function boot()
    {
        $source = realpath(__DIR__.'/config/bigquery.php');
        $this->publishes([
            $source => config_path('bigquery.php')
        ], 'laravel-bigquery');
    }

    /**
     * Register any package services.
     *
     * @return void
     */
    public function register()
    {
        $this->mergeConfigFrom(__DIR__.'/config/bigquery.php', 'bigquery');

        $this->app->bind(BigQuery::class, BigQuery::class);
    }

    protected function guardAgainstInvalidConfiguration(array $bigQueryConfig = null)
    {
        if (! file_exists($bigQueryConfig['application_credentials'])) {
            throw InvalidConfiguration::credentialsJsonDoesNotExist($bigQueryConfig['application_credentials']);
        }
    }

 

    /**
     * @return array
     */
    public function provides()
    {
        return [BigQueryClient::class];
    }
}
