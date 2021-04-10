<?php

namespace App\Entity;

use App\Repository\StockRepository;
use Doctrine\ORM\Mapping as ORM;

/**
 * @ORM\Entity(repositoryClass=StockRepository::class)
 */
class Stock {

    /**
     * @ORM\Id
     * @ORM\GeneratedValue
     * @ORM\Column(type="integer")
     */
    private $id;

    /**
     * @ORM\Column(type="string", length=255, unique=true)
     */
    private $tag;

    /**
     * @ORM\Column(type="string", length=255)
     */
    private $name;

    /**
     * @ORM\Column(type="decimal", precision=10, scale=2)
     */
    private $priceInDollar;

    /**
     * @ORM\Column(type="boolean")
     */
    private $isActive;

    public function getId(): ?int {
        return $this->id;
    }

    public function getTag(): ?string {
        return $this->tag;
    }

    public function setTag(string $tag): self {
        $this->tag = $tag;

        return $this;
    }

    public function getName(): ?string {
        return $this->name;
    }

    public function setName(string $name): self {
        $this->name = $name;

        return $this;
    }

    public function getPriceInDollar(): ?string {
        return $this->priceInDollar;
    }

    public function setPriceInDollar(string $price_in_dollar): self {
        $this->priceInDollar = $price_in_dollar;

        return $this;
    }

    public function getIsActive(): ?bool {
        return $this->isActive;
    }

    public function setIsActive(bool $is_active): self {
        $this->isActive = $is_active;

        return $this;
    }
}