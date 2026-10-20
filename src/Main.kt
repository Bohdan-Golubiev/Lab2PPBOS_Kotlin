import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.CyclicBarrier
import kotlin.concurrent.thread
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import java.util.concurrent.Callable
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger

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

    suspend fun run(): Long = coroutineScope {

        val commands = List(workers) { Channel<Int>(Channel.RENDEZVOUS) }
        val done = Channel<Unit>(workers)

        val jobs = commands.mapIndexed { id, cmd ->
            async(Dispatchers.Default.limitedParallelism(workers)) {
                for (n in cmd) {
                    val pairs = n / 2
                    var i = id
                    while (i < pairs) {
                        arr[i] += arr[n - 1 - i]
                        i += workers
                    }
                    done.send(Unit)
                }
            }
        }

        var n = arr.size
        while (n > 1) {
            commands.forEach { it.send(n) }
            repeat(workers) { done.receive() }
            n = (n + 1) / 2
        }

        commands.forEach { it.close() }
        jobs.awaitAll()
        arr[0]
    }
}

class WaveSumExecutor( private val arr: LongArray, private val workers: Int, private val chunk: Int = 4096) {
    private val pool: ExecutorService = Executors.newFixedThreadPool(workers)

    fun run(): Long {
        try {
            var len = arr.size
            while (len > 1) {
                val pairs = len / 2
                val last = len - 1

                if (pairs <= chunk) {
                    for (i in 0 until pairs) arr[i] += arr[last - i]
                } else {
                    val next = AtomicInteger(0)
                    val taskCount = minOf(workers, (pairs + chunk - 1) / chunk)
                    val tasks = List(taskCount) {
                        Callable{
                            while (true) {
                                val from = next.getAndAdd(chunk)
                                if (from >= pairs) break
                                val to = minOf(from + chunk, pairs)
                                for (i in from until to) arr[i] += arr[last - i]
                            }
                        }
                    }
                    pool.invokeAll(tasks).forEach { it.get() }
                }
                len -= pairs
            }
            return arr[0]
        } finally {
            pool.shutdown()
        }
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
    val end = System.nanoTime()
    println("result=$result \nexpected=$expected \ntime=${(end - start) /  1_000_000} ms")*/

    /*val start = System.nanoTime()
    val result = WaveSumExecutor(array, threads).run()
    val end = System.nanoTime()
    println("result=$result \nexpected=$expected \ntime=${(end - start) /  1_000_000} ms")*/

}