import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.CyclicBarrier
import kotlin.concurrent.thread
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel

const val WAVE_END = -1
const val STOP = -2

class WaveSum(private val arr: LongArray, private val workers: Int) {

    @Volatile private var n = arr.size
    private val queue = ArrayBlockingQueue<Int>(1024)
    private val barrier = CyclicBarrier(workers + 1)

    private fun worker() {
        while (true) {
            when (val i = queue.take()) {
                STOP -> return
                WAVE_END -> barrier.await()
                else -> arr[i] += arr[n - 1 - i]
            }
        }
    }

    fun run(): Long {
        val threads = List(workers) { thread { worker() } }

        while (n > 1) {
            val pairs = n / 2
            for (i in 0 until pairs) queue.put(i)
            repeat(workers) { queue.put(WAVE_END) }
            barrier.await()
            n = (n + 1) / 2
        }

        repeat(workers) { queue.put(STOP) }
        threads.forEach { it.join() }
        return arr[0]
    }
}
class WaveSumCoroutine(private val arr: LongArray, private val workers: Int) {

    private val chunk = 4096

    suspend fun run(): Long = coroutineScope {
        var n = arr.size

        while (n > 1) {
            val pairs = n / 2
            val size = n
            val channel = Channel<Int>(1024)

            val jobs = List(workers) {
                async(Dispatchers.IO) {
                    for (index in channel) {
                        val end = minOf(index + chunk, pairs)
                        for (i in index until end) arr[i] += arr[size - 1 - i]

                        //arr[index] += arr[n - 1 - index]
                    }
                }
            }

            for (start in 0 until pairs step chunk) channel.send(start)
            channel.close()

            jobs.awaitAll()
            n = (n + 1) / 2
        }

        arr[0]
    }
}

fun main() {
    val size = 1_000_000_000
    val threads = Runtime.getRuntime().availableProcessors()-1

    val array = LongArray(size) { it + 1L }
    val expected = size.toLong() * (size + 1) / 2

    runBlocking {
        val start = System.nanoTime()
        val result = WaveSumCoroutine(array, threads).run()
        val end = System.nanoTime()
        println("result=$result \nexpected=$expected \ntime=${(end - start) /  1_000_000} ms")
    }

    /*val start = System.nanoTime()
    val result = WaveSum(array, threads).run()
    val end = System.nanoTime()*/
}