<?php

declare(strict_types=1);

namespace Wwwision\DCBEventStoreESDB;

use DateTimeImmutable;
use Psr\Clock\ClockInterface;
use RuntimeException;
use Thenativeweb\Eventsourcingdb\Client;
use Wwwision\DCBEventStore\EventStore;
use Wwwision\DCBEventStore\Exceptions\ConditionalAppendFailed;
use Wwwision\DCBEventStore\Types\AppendCondition;
use Wwwision\DCBEventStore\Types\Event;
use Wwwision\DCBEventStore\Types\Events;
use Wwwision\DCBEventStore\Types\ReadOptions;
use Wwwision\DCBEventStore\Types\StreamQuery\Criteria\EventTypesAndTagsCriterion;
use Wwwision\DCBEventStore\Types\StreamQuery\StreamQuery;

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

    public function read(StreamQuery $query, ?ReadOptions $options = null): ESDBEventStream
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
        }
        $eventQLParts[] = 'PROJECT INTO { type: SUBSTRING(e.type, ' . strlen($this->eventTypePrefix) . '), tags: e.data.tags, data: e.data.payload, recorded_at: e.data.recorded_at, position: (e.id AS INT + 1) }';

        return new ESDBEventStream($this->client->runEventQlQuery(implode(' ', $eventQLParts)));
    }

    public function append(Events|Event $events, AppendCondition $condition): void
    {
        $convertedEvents = $events instanceof Events ? $events->map($this->convertEvent(...)) : [$this->convertEvent($events)];
        try {
            $this->client->writeEvents($convertedEvents, $this->convertAppendCondition($condition));
        } catch (RuntimeException $exception) {
            // FIXME this hack is required because {@see Client::writeEvents} does not allow to detect this case otherwise
            if ($exception->getMessage() === 'Failed to write events, got HTTP status code \'409\', expected \'200\'') {
                $exception = $condition->expectedHighestSequenceNumber->isNone() ? ConditionalAppendFailed::becauseNoEventWhereExpected() : ConditionalAppendFailed::becauseHighestExpectedSequenceNumberDoesNotMatch($condition->expectedHighestSequenceNumber);
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
            ],
        ];
    }

    /**
     * @return array<int, mixed>
     */
    private function convertAppendCondition(AppendCondition $condition): array
    {
        if ($condition->expectedHighestSequenceNumber->isAny()) {
            return [];
        }
        $eventQLParts = ['FROM e IN events'];
        $eventQLConditions = $this->convertQueryToEventQLConditions($condition->query);
        if (!$condition->expectedHighestSequenceNumber->isNone()) {
            $eventQLConditions[] = '(e.id AS INT + 1) > ' . $condition->expectedHighestSequenceNumber->extractSequenceNumber()->value;
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
    private function convertQueryToEventQLConditions(StreamQuery $query, bool|null &$onlyLastEvent = null): array
    {
        if ($query->isWildcard()) {
            return [];
        }
        $numberOfCriteria = 0;
        $eventQLParts = $query->criteria->map(function (EventTypesAndTagsCriterion $c) use (&$onlyLastEvent, &$numberOfCriteria) {
            $numberOfCriteria ++;
            if ($c->onlyLastEvent) {
                $onlyLastEvent = true;
            }
            $subParts = [];
            if ($c->eventTypes !== null) {
                $subParts[] = '["' . implode('","', array_map(static fn(string $type) => "events.dcb.$type", $c->eventTypes->toStringArray())) . '"] CONTAINS e.type';
            }
            if ($c->tags !== null) {
                $subParts[] = implode(' AND ', array_map(static fn(string $tag) => "e.data.tags CONTAINS \"$tag\"", $c->tags->toStrings()));
            }
            return implode(' AND ', $subParts);
        });
        if ($onlyLastEvent && $numberOfCriteria > 1) {
            throw new RuntimeException('This adapter supports the "onlyLastEvent" flag only for queries that contain a single criterion');
        }
        return [implode(') OR (', $eventQLParts)];
    }
}
