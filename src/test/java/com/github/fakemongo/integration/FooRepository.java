package com.github.fakemongo.integration;

import org.springframework.data.mongodb.repository.MongoRepository;

public interface FooRepository extends MongoRepository<SpringFongoTest.Foo, String> {
}
