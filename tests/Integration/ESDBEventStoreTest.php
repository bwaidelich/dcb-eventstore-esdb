<?php
declare(strict_types=1);

namespace Wwwision\DCBEventStoreESDB\Tests\Integration;

use PHPUnit\Framework\Attributes\CoversClass;
use RuntimeException;
use Thenativeweb\Eventsourcingdb\Container;
use Wwwision\DCBEventStore\Query\Query;
use Wwwision\DCBEventStore\Query\QueryItem;
use Wwwision\DCBEventStore\Tests\Integration\EventStoreTestBase;
use Wwwision\DCBEventStoreESDB\ESDBEventStore;

#[CoversClass(ESDBEventStore::class)]
final class ESDBEventStoreTest extends EventStoreTestBase
{
    private Container|null $testContainer = null;

    protected function createEventStore(): ESDBEventStore
    {
        if ($this->testContainer === null) {
            $this->testContainer = (new Container())->withImageTag('preview');
            $this->testContainer->start();
        }
        return ESDBEventStore::createForClient($this->testContainer->getClient())->with(
            clock: $this->getTestClock()
        );
    }

    public function tearDown(): void
    {
        $this->testContainer?->stop();
    }

    public function test_read_does_not_fail_if_onlyLastEvent_flag_is_used_in_query_with_single_item(): void
    {
        $query = Query::fromItems(QueryItem::create(eventTypes: ['SomeEventType'], tags: ['foo:bar'], onlyLastEvent: true));
        $eventStream = $this->createEventStore()->read($query);
        self::assertEventStream($eventStream, []);
    }

    public function test_read_fails_if_onlyLastEvent_flag_is_used_in_query_with_multiple_items(): void
    {
        $query = Query::fromItems(
            QueryItem::create(eventTypes: ['SomeEventType'], tags: ['foo:bar'], onlyLastEvent: true),
            QueryItem::create(eventTypes: ['SomeEventType'], tags: ['foo:bar']),
        );

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('This adapter supports the "onlyLastEvent" flag only for queries that contain a single criterion');
        $this->createEventStore()->read($query);
    }

    public function test_append_does_not_fail_if_onlyLastEvent_flag_is_used_in_condition_query_with_single_item(): void
    {
        $this->expectNotToPerformAssertions();
        $query = Query::fromItems(QueryItem::create(eventTypes: ['SomeEventType'], tags: ['foo:bar'], onlyLastEvent: true));
        $this->conditionalAppendEvent(['type' => 'SomeEventType', 'data' => 'some event', 'tags' => ['baz:foos']], $query);
    }

    public function test_append_fails_if_onlyLastEvent_flag_is_used_in_condition_query_with_multiple_items(): void
    {
        $query = Query::fromItems(
            QueryItem::create(eventTypes: ['SomeEventType'], tags: ['foo:bar'], onlyLastEvent: true),
            QueryItem::create(eventTypes: ['SomeEventType'], tags: ['foo:bar']),
        );

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('This adapter supports the "onlyLastEvent" flag only for queries that contain a single criterion');
        $this->conditionalAppendEvent(['type' => 'SomeEventType', 'data' => 'some event', 'tags' => ['baz:foos']], $query);
    }

}