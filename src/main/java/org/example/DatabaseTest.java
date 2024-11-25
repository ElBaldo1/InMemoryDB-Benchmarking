package org.example;


import redis.clients.jedis.Jedis;
import net.spy.memcached.MemcachedClient;

import java.net.InetSocketAddress;

public class DatabaseTest {
    public static void main(String[] args) {
        // Redis Connection
        try (Jedis jedis = new Jedis("localhost", 6379)) {
            System.out.println("Connected to Redis");

        } catch (Exception e) {
            System.out.println("Error with Redis: " + e.getMessage());
        }

        // Memcached Connection
        try {
            MemcachedClient memcachedClient = new MemcachedClient(new InetSocketAddress("localhost", 11211));
            System.out.println("Connected to Memcached");

        } catch (Exception e) {
            System.out.println("Error with Memcached: " + e.getMessage());
        }
    }
}
