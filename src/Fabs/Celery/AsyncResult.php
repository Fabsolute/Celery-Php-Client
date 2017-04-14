<?php
/**
 * Created by PhpStorm.
 * User: ahmetturk
 * Date: 14/04/2017
 * Time: 13:27
 */

namespace Fabs\Celery;


use Fabs\Celery\Exception\CeleryException;
use PhpAmqpLib\Connection\AMQPStreamConnection;
use PhpAmqpLib\Message\AMQPMessage;

class AsyncResult
{
    /**
     * @var string
     */
    private $task_id = null;
    /**
     * @var AMQPStreamConnection
     */
    private $connection = null;
    /**
     * @var null
     */
    private $result = null;

    public function __construct($task_id, $connection)
    {
        $this->task_id = $task_id;
        $this->connection = $connection;
    }

    private function getAsyncResult()
    {
        if ($this->result != null) {
            return $this->result;
        }
        try {
            $channel = $this->connection->channel();
            $output = false;
            /** @var AMQPMessage $message */
            $message = $channel->basic_get($this->task_id);
            if ($message != null) {
                $this->result = json_decode($message->getBody(), true);
                $channel->queue_delete($this->task_id);
                $output = $this->result;
            }

            $channel->close();
            return $output;
        } catch (\Exception $e) {
            return false;
        }
    }

    private function getBody()
    {
        if ($this->result == null) {
            throw  new CeleryException('task not ready yet');
        }
        return $this->result;
    }

    public function isReady()
    {
        return $this->getAsyncResult() !== false;
    }

    public function getStatus()
    {
        $result = $this->getBody();
        return $result['status'];
    }

    public function isSuccess()
    {
        return $this->getStatus() == 'SUCCESS';
    }

    public function getTraceBack()
    {
        $result = $this->getBody();
        return $result['traceback'];
    }

    public function getResult()
    {
        $result = $this->getBody();
        return $result['result'];
    }

    public function isFailed()
    {
        return $this->isReady() && !$this->isSuccess();
    }
}