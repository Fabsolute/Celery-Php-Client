<?php
/**
 * Created by PhpStorm.
 * User: ahmetturk
 * Date: 14/04/2017
 * Time: 12:34
 */

namespace Fabs\Celery;


use Fabs\Celery\Exception\CeleryException;

use Fabs\Celery\Exception\CeleryPublishException;
use PhpAmqpLib\Connection\AMQPStreamConnection;
use PhpAmqpLib\Message\AMQPMessage;
use PhpAmqpLib\Wire\AMQPTable;

class Celery
{
    /**
     * @var AMQPStreamConnection
     */
    private $connection = null;
    /**
     * @var string
     */
    private $exchange_name = '';
    /**
     * @var string
     */
    private $route_key = '';

    public function __construct($amqp_host, $amqp_port, $amqp_user, $amqp_password,
                                $exchange_name = 'celery', $route_key = 'celery')
    {
        $this->connection = new AMQPStreamConnection($amqp_host, $amqp_port, $amqp_user, $amqp_password);
        $this->exchange_name = $exchange_name;
        $this->route_key = $route_key;
    }

    public function postTask($task_name, $arguments, $task_arguments = [])
    {
        try {
            if (!is_array($arguments)) {
                throw new CeleryException('arguments must be an array');
            }

            $unique_id = uniqid('celery_client_', true);

            if (array_keys($arguments) == range(0, count($arguments) - 1)) {
                $kw_arguments = [];
            } else {
                $kw_arguments = $arguments;
                $arguments = [];
            }

            $message = [
                $arguments,
                (object)$kw_arguments, [
                    "chord" => null,
                    "callbacks" => null,
                    "errbacks" => null,
                    "chain" => null
                ]
            ];

            $kw_arguments_content = $this->kw_repr($kw_arguments);
            $arguments_content = $this->repr($arguments);

            $task_message = array_merge([
                'id' => $unique_id,
                'task' => $task_name,
                'kwargsrepr' => $kw_arguments_content,
                'argsrepr' => $arguments_content
            ], $task_arguments);

            $task = json_encode($message, JSON_UNESCAPED_UNICODE);

            $parameters = [
                'content_type' => 'application/json',
                'content_encoding' => 'UTF-8',
                'immediate' => false,
                'delivery_mode' => 2
            ];

            $amqp_message = new AMQPMessage($task, $parameters);

            $headers = new AMQPTable($task_message);

            $amqp_message->set('application_headers', $headers);

            $channel = $this->connection->channel();
            $channel->basic_publish($amqp_message, $this->exchange_name, $this->route_key);

            return new AsyncResult($unique_id, $this->connection, $arguments);
        } catch (\Exception $e) {
            throw  new CeleryPublishException();
        }
    }

    private function kw_repr($kw_arguments)
    {
        $kw_arguments_content = json_encode($kw_arguments);
        if (strpos($kw_arguments_content, '{') !== 0) {
            if (strpos($kw_arguments_content, '[') === 0) {
                $kw_arguments_content = '';
            }
            $kw_arguments_content = '{' . $kw_arguments_content . '}';
        }
        $kw_arguments_content = str_replace("\"", "'", $kw_arguments_content);
        return $kw_arguments_content;
    }

    private function repr($arguments)
    {
        $arguments_content = json_encode($arguments);
        if (strpos($arguments_content, '[') === 0) {
            $arguments_content = substr($arguments_content, 1);
            $arguments_content = substr($arguments_content, 0, -1);
        }
        $arguments_content = '(' . $arguments_content . ')';
        $arguments_content = str_replace("\"", "'", $arguments_content);
        return $arguments_content;
    }
}