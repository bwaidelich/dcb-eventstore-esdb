<?php
declare(strict_types=1);

namespace Wwwision\DCBEventStoreESDB\Tests\Integration;

use PHPUnit\Framework\Attributes\CoversNothing;
use RuntimeException;
use Thenativeweb\Eventsourcingdb\Container;
use Wwwision\DCBEventStore\EventStore;
use Wwwision\DCBEventStore\Query\Query;
use Wwwision\DCBEventStore\Tests\Integration\EventStoreConcurrencyTestBase;
use Wwwision\DCBEventStoreESDB\ESDBEventStore;

require_once __DIR__ . '/../../vendor/autoload.php';

#[CoversNothing]
final class ConcurrencyTest extends EventStoreConcurrencyTestBase
{

    private static ESDBEventStore|null $eventStore = null;
    private static Container|null $testContainer = null;

    public static function prepare(): void
    {
        $eventStore = self::createEventStore();
        if ($eventStore->read(Query::all())->first() !== null) {
            throw new RuntimeException('The event store must not contain any events before preforming consistency tests');
        }
    }

    public static function cleanup(): void
    {
        self::$testContainer?->stop();
        self::$testContainer = null;
        putenv('DCB_TEST_ESDB_URL');
        putenv('DCB_TEST_ESDB_API_KEY');
    }

    protected static function createEventStore(): EventStore
    {
        if (self::$eventStore === null) {
            $esdbUrl = getenv('DCB_TEST_ESDB_URL');
            if (!is_string($esdbUrl)) {
                self::$testContainer = (new Container())->withImageTag('preview');
                self::$testContainer->start();
                $esdbUrl = self::$testContainer->getBaseUrl();
                putenv('DCB_TEST_ESDB_URL=' . $esdbUrl);
            }
            self::$eventStore = ESDBEventStore::create($esdbUrl, getenv('DCB_TEST_ESDB_API_KEY') ?: 'secret');
        }
        return self::$eventStore;
    }

}