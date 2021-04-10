<?php

namespace App\Controller;

use App\Service\EtoroService;
use Symfony\Component\HttpFoundation\JsonResponse;
use Symfony\Component\HttpFoundation\Response;
use Symfony\Component\Routing\Annotation\Route;

class TestController {

    /**
     * @var EtoroService
     */
    private $etoroService;

    public function __construct(EtoroService $etoro_service) {
        $this->etoroService = $etoro_service;
    }

    /**
     * @Route("/test")
     * @return Response
     */
    public function test(): Response {
        return new JsonResponse($this->etoroService->getPortfolioSummary());
    }
}