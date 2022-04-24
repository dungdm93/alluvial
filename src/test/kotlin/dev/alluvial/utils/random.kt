package dev.alluvial.utils

import java.util.Random


fun <E> Array<out E>.random(random: Random): E {
    val idx = random.nextInt(size)
    return get(idx)
}

fun <E> List<E>.random(random: Random): E {
    val idx = random.nextInt(size)
    return get(idx)
}

fun <E> Iterable<E>.random(random: Random): E {
    return this.toList().random(random)
}

fun <E> Iterable<E>.randomSublist(random: Random): List<E> {
    val list = this.shuffled(random)
    val size = random.nextInt(list.size)

    return list.take(size)
}

fun <E> Array<out E>.randomSublist(random: Random): List<E> {
    val list = this.toMutableList().apply { shuffle(random) }
    val size = random.nextInt(list.size)

    return list.take(size)
}
