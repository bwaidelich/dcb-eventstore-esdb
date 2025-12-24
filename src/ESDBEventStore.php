<?php

declare(strict_types=1);

namespace Wwwision\DCBEventStoreESDB;

use DateTimeImmutable;
use Psr\Clock\ClockInterface;
use RuntimeException;
use Thenativeweb\Eventsourcingdb\Client;
use Webmozart\Assert\Assert;
use Wwwision\DCBEventStore\AppendCondition\AppendCondition;
use Wwwision\DCBEventStore\Event\Event;
use Wwwision\DCBEventStore\Event\Events;
use Wwwision\DCBEventStore\EventStore;
use Wwwision\DCBEventStore\Exceptions\ConditionalAppendFailed;
use Wwwision\DCBEventStore\Query\Query;
use Wwwision\DCBEventStore\Query\QueryItem;
use Wwwision\DCBEventStore\ReadOptions;
use Wwwision\DCBEventStore\SequencedEvent\SequencedEvent;
use Wwwision\DCBEventStore\SequencedEvent\SequencedEvents;
use Wwwision\DCBEventStore\SequencedEvent\SequencePosition;

final readonly class ESDBEventStore implements EventStore
{
    private ClockInterface $clock;

    private function __construct(
        private Client $client,
        private string $eventSource = 'https://dcb.events',
        private string $eventTypePrefix = 'events.dcb.',
        ClockInterface|null $clock = null,
    ) {
        $this->clock = $clock ?? new class implements ClockInterface {
            public function now(): DateTimeImmutable
            {
                return new DateTimeImmutable();
            }
        };
    }

    public static function create(string $baseUri, string $apiKey): self
    {
        return new self(new Client($baseUri, $apiKey));
    }

    public static function createForClient(Client $client): self
    {
        return new self($client);
    }

    public function with(
        ClockInterface|null $clock = null,
        string|null $eventSource = null,
        string|null $eventTypePrefix = null,
    ): self {
        return new self(
            client: $this->client,
            eventSource: $eventSource ?? $this->eventSource,
            eventTypePrefix: $eventTypePrefix ?? $this->eventTypePrefix,
            clock: $clock ?? $this->clock
        );
    }

    public function ping(): void
    {
        $this->client->ping();
    }

    public function read(Query $query, ReadOptions|null $options = null): SequencedEvents
    {
        $eventQLParts = ['FROM e IN events'];
        $eventQLConditions = $this->convertQueryToEventQLConditions($query, $onlyLastEvent);
        if ($options !== null && $options->from !== null) {
            $fromValue = $options->from->value;
            $eventQLConditions[] = $options->backwards ? "(e.id AS INT + 1) <= $fromValue" : "(e.id AS INT + 1) >= $fromValue";
        }
        if ($eventQLConditions !== []) {
            $eventQLParts[] = 'WHERE (' . implode(') AND (', $eventQLConditions) . ')';
        }
        if ($options?->backwards === true || $onlyLastEvent) {
            $eventQLParts[] = 'ORDER BY e.id AS INT DESC';
        }
        if ($onlyLastEvent) {
            $eventQLParts[] = 'TOP 1';
        } elseif ($options?->limit !== null) {
            $eventQLParts[] = 'TOP ' . $options->limit;
        }
        $eventQLParts[] = 'PROJECT INTO { type: SUBSTRING(e.type, ' . strlen($this->eventTypePrefix) . '), tags: e.data.tags, data: e.data.payload, metadata: e.data.metadata, recorded_at: e.data.recorded_at, position: (e.id AS INT + 1) }';

        $result = $this->client->runEventQlQuery(implode(' ', $eventQLParts));
        return SequencedEvents::create(static function () use ($result) {
            foreach ($result as $event) {
                yield self::convertESDBEvent($event);
            }
        });
    }

    public function append(Events|Event $events, AppendCondition|null $condition = null): void
    {
        $convertedEvents = $events instanceof Events ? $events->map($this->convertEvent(...)) : [$this->convertEvent($events)];
        $preconditions = $condition !== null ? self::convertAppendCondition($condition) : [];
        try {
            $this->client->writeEvents($convertedEvents, $preconditions);
        } catch (RuntimeException $exception) {
            // FIXME this hack is required because {@see Client::writeEvents} does not allow to detect this case otherwise
            if ($condition !== null && $exception->getMessage() === 'Failed to write events, got HTTP status code \'409\', expected \'200\'') {
                $exception = $condition->after === null ? ConditionalAppendFailed::becauseMatchingEventsExist() : ConditionalAppendFailed::becauseMatchingEventsExistAfterSequencePosition($condition->after);
            }
            throw $exception;
        }
    }

    /**
     * @return array<string, mixed>
     */
    private function convertEvent(Event $event): array
    {
        return [
            'source' => $this->eventSource,
            'subject' => '/',
            'type' => $this->eventTypePrefix . $event->type->value,
            'data' => [
                'payload' => $event->data->value,
                'tags' => $event->tags->toStrings(),
                'recorded_at' => $this->clock->now()->format('Y-m-d H:i:s'),
                'metadata' => $event->metadata->value,
            ],
        ];
    }

    /**
     * @return array<int, mixed>
     */
    private static function convertAppendCondition(AppendCondition $condition): array
    {
        $eventQLParts = ['FROM e IN events'];
        $eventQLConditions = self::convertQueryToEventQLConditions($condition->failIfEventsMatch);
        if ($condition->after !== null) {
            $eventQLConditions[] = '(e.id AS INT + 1) > ' . $condition->after->value;
        }
        if ($eventQLConditions !== []) {
            $eventQLParts[] = 'WHERE (' . implode(') AND (', $eventQLConditions) . ')';
        }
        $eventQLParts[] = 'PROJECT INTO COUNT() == 0';
        return [
            [
                'type' => 'isEventQlQueryTrue',
                'payload' => [
                    'query' => implode(' ', $eventQLParts)
                ]
            ]
        ];
    }

    /**
     * @return array<int, string>
     */
    private static function convertQueryToEventQLConditions(Query $query, bool|null &$onlyLastEvent = null): array
    {
        if (!$query->hasItems()) {
            return [];
        }
        $numberOfCriteria = 0;
        $eventQLParts = $query->map(function (QueryItem $item) use (&$onlyLastEvent, &$numberOfCriteria) {
            $numberOfCriteria ++;
            if ($item->onlyLastEvent) {
                $onlyLastEvent = true;
            }
            $subParts = [];
            if ($item->eventTypes !== null) {
                $subParts[] = '["' . implode('","', array_map(static fn(string $type) => "events.dcb.$type", $item->eventTypes->toStringArray())) . '"] CONTAINS e.type';
            }
            if ($item->tags !== null) {
                $subParts[] = implode(' AND ', array_map(static fn(string $tag) => "e.data.tags CONTAINS \"$tag\"", $item->tags->toStrings()));
            }
            return implode(' AND ', $subParts);
        });
        if ($onlyLastEvent && $numberOfCriteria > 1) {
            throw new RuntimeException('This adapter supports the "onlyLastEvent" flag only for queries that contain a single criterion');
        }
        return [implode(') OR (', $eventQLParts)];
    }

    /**
     * @param array<mixed> $event
     */
    private static function convertESDBEvent(array $event): SequencedEvent
    {
        Assert::numeric($event['position']);
        $recordedAt = DateTimeImmutable::createFromFormat('Y-m-d H:i:s', $event['recorded_at']);
        Assert::isInstanceOf($recordedAt, DateTimeImmutable::class);
        return new SequencedEvent(
            SequencePosition::fromInteger((int)$event['position']),
            $recordedAt,
            Event::create(
                $event['type'],
                $event['data'],
                $event['tags'],
                $event['metadata'],
            ),
        );
    }
}
