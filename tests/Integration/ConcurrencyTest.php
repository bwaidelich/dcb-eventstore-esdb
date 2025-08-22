<?php
declare(strict_types=1);

namespace Wwwision\DCBEventStoreESDB\Tests\Integration;

use PHPUnit\Framework\Attributes\CoversNothing;
use RuntimeException;
use Wwwision\DCBEventStore\EventStore;
use Wwwision\DCBEventStore\Tests\Integration\EventStoreConcurrencyTestBase;
use Wwwision\DCBEventStore\Types\StreamQuery\StreamQuery;
use Wwwision\DCBEventStoreESDB\ESDBEventStore;

require_once __DIR__ . '/../../vendor/autoload.php';

#[CoversNothing]
final class ConcurrencyTest extends EventStoreConcurrencyTestBase
{

    private static ESDBEventStore|null $eventStore = null;

    public static function prepare(): void
    {
        $eventStore = self::createEventStore();
        if ($eventStore->read(StreamQuery::wildcard())->first() !== null) {
            throw new RuntimeException('The event store must not contain any events before preforming consistency tests');
        }
    }

    public static function cleanup(): void
    {
        // nothing to do here
    }

    protected static function createEventStore(): EventStore
    {
        if (self::$eventStore === null) {
            $baseUri = getenv('DCB_TEST_BASE_URI');
            if (empty($baseUri) || !is_string($baseUri)) {
                throw new RuntimeException('Missing/invalid environment variable DCB_TEST_BASE_URI');
            }
            $apiKey = getenv('DCB_TEST_API_KEY');
            if (empty($apiKey) || !is_string($apiKey)) {
                throw new RuntimeException('Missing/invalid environment variable DCB_TEST_API_KEY');
            }
            self::$eventStore = ESDBEventStore::create($baseUri, $apiKey);
        }
        return self::$eventStore;
    }

}