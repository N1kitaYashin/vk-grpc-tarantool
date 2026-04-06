package com.example.demo.kv;

import java.util.Optional;

public record KvGetResult(boolean found, Optional<byte[]> value) {
}