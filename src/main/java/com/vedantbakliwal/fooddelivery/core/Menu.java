package com.vedantbakliwal.fooddelivery.core;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Restaurants and their menus (invented). Prices always come from here, never from the client.
 * "Night Owl Noodles" is closed and "Taco Lab" is out of one item, so rejections can be tested.
 */
public final class Menu {

    public record Item(String sku, String name, long priceCents) { }

    public record Restaurant(String id, String name, String zone, boolean open, Map<String, Item> items, Set<String> soldOut) { }

    public static final List<String> ZONES = List.of("downtown", "east", "north", "south");

    private static final Map<String, Restaurant> RESTAURANTS = Map.of(
            "r-taco-lab", new Restaurant("r-taco-lab", "Taco Lab", "east", true, Map.of(
                    "taco-al-pastor", new Item("taco-al-pastor", "Al pastor taco", 450),
                    "taco-veggie", new Item("taco-veggie", "Veggie taco", 400),
                    "elote", new Item("elote", "Elote", 550)), Set.of("elote")),
            "r-brisket-co", new Restaurant("r-brisket-co", "Brisket Co", "downtown", true, Map.of(
                    "brisket-plate", new Item("brisket-plate", "Brisket plate", 1895),
                    "mac-cheese", new Item("mac-cheese", "Mac and cheese", 595)), Set.of()),
            "r-night-owl", new Restaurant("r-night-owl", "Night Owl Noodles", "north", false, Map.of(
                    "pho", new Item("pho", "Beef pho", 1450)), Set.of()));

    private Menu() {
    }

    public static Optional<Restaurant> restaurant(String id) {
        return Optional.ofNullable(RESTAURANTS.get(id));
    }

    public static List<Restaurant> all() {
        return RESTAURANTS.values().stream().sorted((a, b) -> a.id().compareTo(b.id())).toList();
    }

    /** Distance in "blocks" between zones; dispatch prefers the nearest courier. */
    public static int distance(String a, String b) {
        return Math.abs(ZONES.indexOf(a) - ZONES.indexOf(b));
    }
}
