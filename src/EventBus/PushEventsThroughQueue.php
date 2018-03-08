<?php declare(strict_types=1);

namespace SmoothPhp\LaravelAdapter\EventBus;

use Illuminate\Contracts\Config\Repository;
use Illuminate\Contracts\Queue\Queue;
use SmoothPhp\Contracts\Domain\DomainMessage;
use SmoothPhp\Contracts\EventBus\EventListener;
use SmoothPhp\Contracts\Serialization\Serializer;

/**
 * Class PushEventsThroughQueue
 * @package SmoothPhp\LaravelAdapter\EventBus
 * @author Simon Bennett <simon@pixelatedcrow.com>
 */
final class PushEventsThroughQueue implements EventListener
{
    /** @var Queue */
    private $queue;

    /** @var Serializer */
    private $serializer;
    /** @var Repository */
    private $config;

    /**
     * PushEventsThroughQueue constructor.
     * @param Queue $queue
     * @param Serializer $serializer
     * @param Repository $config
     */
    public function __construct(Queue $queue, Serializer $serializer, Repository $config)
    {
        $this->queue = $queue;
        $this->serializer = $serializer;
        $this->config = $config;
    }

    /**
     * @param DomainMessage $domainMessage
     */
    public function handle(DomainMessage $domainMessage)
    {
        if(!$domainMessage->getPayload() instanceof ShouldQueue && $this->config->get('cqrses.require_should_queue_class', false)) {
            return $this->dispatchNow($domainMessage);
        }

        return $this->dispatchToQueue($domainMessage);
    }

    /**
     * @param DomainMessage $domainMessage
     */
    private function dispatchNow(DomainMessage $domainMessage)
    {
        \Log::info("Dispatching Immediately: " . get_class($domainMessage->getPayload()));

        $dispatcher = app(EventDispatcher::class);

        $payload = $this->serializer->serialize($domainMessage->getPayload());

        $event = call_user_func([
            str_replace('.', '\\', $domainMessage->getType()),
            'deserialize'
        ],
            $payload['payload']);

        return $dispatcher->dispatch($domainMessage->getType(), [$event]);
    }

    /**
     * @param DomainMessage $domainMessage
     */
    private function dispatchToQueue(DomainMessage $domainMessage)
    {
        $this->queue->push(
            QueueToEventDispatcher::class,
            [
                'uuid'        => (string)$domainMessage->getId(),
                'playhead'    => $domainMessage->getPlayHead(),
                'metadata'    => json_encode($this->serializer->serialize($domainMessage->getMetadata())),
                'payload'     => json_encode($this->serializer->serialize($domainMessage->getPayload())),
                'recorded_on' => (string)$domainMessage->getRecordedOn(),
                'type'        => $domainMessage->getType(),
            ],
            $this->config->get('cqrses.queue_name', 'default')
        );
    }
}