package cn.wanghaomiao.seimi.core;

import cn.wanghaomiao.seimi.Constants;
import cn.wanghaomiao.seimi.def.BaseSeimiCrawler;
import cn.wanghaomiao.seimi.struct.CrawlerModel;
import cn.wanghaomiao.seimi.struct.Request;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @description: 不同的方式 <br/>
 * @create: 2019-04-10 11:12
 * @author: xs
 * @since JDK1.8
 **/
public class SeimiCrawlerProcessor  implements Runnable {

    private SeimiQueue queue;
    private List<SeimiInterceptor> interceptors;
    private CrawlerModel crawlerModel;
    private BaseSeimiCrawler crawler;
    private Logger logger = LoggerFactory.getLogger(getClass());
    private ThreadPoolExecutor workersPool;

    public SeimiCrawlerProcessor(List<SeimiInterceptor> interceptors, CrawlerModel crawlerModel) {
        this.queue = crawlerModel.getQueueInstance();
        this.interceptors = interceptors;
        this.crawlerModel = crawlerModel;
        this.crawler = crawlerModel.getInstance();
        workersPool = new ThreadPoolExecutor(Constants.BASE_THREAD_NUM, Constants.BASE_THREAD_NUM * 2,
                3, TimeUnit.SECONDS, new LinkedBlockingQueue(Constants.BASE_THREAD_NUM * Runtime.getRuntime().availableProcessors() * 200), new ThreadFactory() {
            private AtomicInteger atomicInteger = new AtomicInteger(0);
            @Override
            public Thread newThread(Runnable r) {
                int count = atomicInteger.incrementAndGet();
                return new Thread(r,crawlerModel.getCrawlerName().concat("-").concat(String.valueOf(count)));
            }
        }, new ThreadPoolExecutor.CallerRunsPolicy());
    }

    @Override
    public void run() {
        while (true) {
            try {
                logger.info("{} 还有 {} 待处理任务。当前排队线程:{},活动线程:{},执行完成线程:{},总线程数:{}",crawlerModel.getCrawlerName(),queue.len(crawlerModel.getCrawlerName()),
                        workersPool.getQueue().size(),workersPool.getActiveCount(),workersPool.getCompletedTaskCount(),workersPool.getTaskCount());
                Request request = queue.bPop(crawlerModel.getCrawlerName());
                if (request == null) {
                    continue;
                }
                logger.debug("SeimiProcessor({}-{})[url:{}]",request.getCrawlerName(),request.getCallBack(),request.getUrl());
                if (crawlerModel == null) {
                    logger.error("No such crawler name:'{}'", request.getCrawlerName());
                    continue;
                }
                if (request.isStop()) {
                    logger.info("SeimiProcessor[{}] will stop!", Thread.currentThread().getName());
                    break;
                }
                workersPool.submit(new SeimiCrawlerHandler(request, crawlerModel, interceptors));
            } catch (Exception e) {
                logger.error("redission queue exception!",e);
                break;
            }
        }
    }
}
