<?php

namespace App\Service;

use Psr\Log\LoggerInterface;
use Psr\Log\LogLevel;
use Symfony\Contracts\HttpClient\Exception\ExceptionInterface;
use Symfony\Contracts\HttpClient\HttpClientInterface;

class EtoroService {

    /**
     * @var HttpClientInterface
     */
    private $httpClient;

    /**
     * @var LoggerInterface
     */
    private $logger;

    private string $subscriptionKey;
    private string $username;

    public function __construct(HttpClientInterface $http_client, LoggerInterface $logger) {
        $this->httpClient = $http_client;
        $this->logger = $logger;

        $this->subscriptionKey = getenv('ETORO_SUBSCRIPTION_KEY');
        $this->username = getenv('ETORO_USERNAME');
    }

    private function request($url): array {

        try {
            $response = $this->httpClient->request('GET', $url, [
                'headers' => [
                    'Ocp-Apim-Subscription-Key' => $this->subscriptionKey
                ],
            ]);

            return $response->toArray();

        } catch(ExceptionInterface $ex) {
            $this->logger->log( LogLevel::ERROR, 'EtoroService request error: ' . $ex->getMessage());
            die();
        }
    }

    public function getPortfolioSummary(?string $username = null): array {
        if(!isset($username)) {
            $username = $this->username;
        }

        return $this->request('https://api.etoro.com/API/User/V1/' . $username . '/PortfolioSummary');
    }
}