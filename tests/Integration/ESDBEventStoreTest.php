<?php
declare(strict_types=1);

namespace Wwwision\DCBEventStoreESDB\Tests\Integration;

use PHPUnit\Framework\Attributes\CoversClass;
use RuntimeException;
use Thenativeweb\Eventsourcingdb\Container;
use Wwwision\DCBEventStore\Tests\Integration\EventStoreTestBase;
use Wwwision\DCBEventStore\Types\ExpectedHighestSequenceNumber;
use Wwwision\DCBEventStore\Types\StreamQuery\Criteria;
use Wwwision\DCBEventStore\Types\StreamQuery\Criteria\EventTypesAndTagsCriterion;
use Wwwision\DCBEventStore\Types\StreamQuery\StreamQuery;
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
        return ESDBEventStore::createForClient($this->testContainer->getClient());
    }

    public function tearDown(): void
    {
        $this->testContainer?->stop();
    }

    public function test_read_does_not_fail_if_onlyLastEvent_flag_is_used_in_query_with_single_item(): void
    {
        $criterion1 = EventTypesAndTagsCriterion::create(eventTypes: ['SomeEventType'], tags: ['foo:bar'], onlyLastEvent: true);
        $query = StreamQuery::create(Criteria::create($criterion1));
        $eventStream = $this->createEventStore()->read($query);
        self::assertEventStream($eventStream, []);
    }

    public function test_read_fails_if_onlyLastEvent_flag_is_used_in_query_with_multiple_items(): void
    {
        $criterion1 = EventTypesAndTagsCriterion::create(eventTypes: ['SomeEventType'], tags: ['foo:bar'], onlyLastEvent: true);
        $criterion2 = EventTypesAndTagsCriterion::create(eventTypes: ['SomeEventType'], tags: ['foo:bar']);
        $query = StreamQuery::create(Criteria::create($criterion1, $criterion2));

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('This adapter supports the "onlyLastEvent" flag only for queries that contain a single criterion');
        $this->createEventStore()->read($query);
    }

    public function test_append_does_not_fail_if_onlyLastEvent_flag_is_used_in_condition_query_with_single_item(): void
    {
        $this->expectNotToPerformAssertions();
        $criterion1 = EventTypesAndTagsCriterion::create(eventTypes: ['SomeEventType'], tags: ['foo:bar'], onlyLastEvent: true);
        $query = StreamQuery::create(Criteria::create($criterion1));
        $this->conditionalAppendEvent(['type' => 'SomeEventType', 'data' => 'some event', 'tags' => ['baz:foos']], $query, ExpectedHighestSequenceNumber::none());
    }

    public function test_append_fails_if_onlyLastEvent_flag_is_used_in_condition_query_with_multiple_items(): void
    {
        $criterion1 = EventTypesAndTagsCriterion::create(eventTypes: ['SomeEventType'], tags: ['foo:bar'], onlyLastEvent: true);
        $criterion2 = EventTypesAndTagsCriterion::create(eventTypes: ['SomeEventType'], tags: ['foo:bar']);
        $query = StreamQuery::create(Criteria::create($criterion1, $criterion2));

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('This adapter supports the "onlyLastEvent" flag only for queries that contain a single criterion');
        $this->conditionalAppendEvent(['type' => 'SomeEventType', 'data' => 'some event', 'tags' => ['baz:foos']], $query, ExpectedHighestSequenceNumber::none());
    }

}