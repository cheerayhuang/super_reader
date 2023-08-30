#include <condition_variable>
#include <functional>
#include <future>
#include <iostream>
#include <memory>
#include <mutex>
#include <queue>
#include <thread>

#include <vector>

template <int N>
class ThreadPool {

using TaskFunc = std::function<void()>;
//using TaskFunc = void();

private:
    std::vector<std::thread> workers;

    std::queue<TaskFunc> tasks_;

    std::mutex task_mutex_;
    std::condition_variable cv_;

    bool stop_ = false;

public:

    ThreadPool() {
        for (auto i = 0; i < N; ++i) {
            workers.emplace_back(
                [this] {
                    for(;;) {
                        TaskFunc task;
                        {
                            std::unique_lock lk(task_mutex_);
                            cv_.wait(lk, [this]{ return !tasks_.empty() || stop_; });
                            if (stop_ && tasks_.empty()) { return; }
                            task = std::move(tasks_.front());
                            tasks_.pop();
                        }

                        task();
                    }
            });
        }
    }

    ~ThreadPool() {
        {
            std::scoped_lock lk(task_mutex_);
            stop_ = true;
        }
        cv_.notify_all();

        for (auto && th : workers) {
            th.join();
        }
    }

    void join() {
        for (auto &&th : workers) {
            th.join();
        }
    }

    template <typename FuncType, typename ...Args>
    auto enqueue(FuncType f, Args... args) {

        using RType = typename std::invoke_result<FuncType, Args...>::type;

        //std::packaged_task<RType()> p_task(std::bind(f, args...));

        // must use pointer to allocate a static memory to store the task.
        std::shared_ptr<std::packaged_task<RType()>> p_task = std::make_shared<std::packaged_task<RType()>>(std::bind(f, args...));
        auto res_feature = p_task->get_future();

        // wrap it to void().
        auto task = [p_task]{ (*p_task)(); };
        {
            std::scoped_lock lk(task_mutex_);

            tasks_.push(task);
        }
        cv_.notify_one();

        return res_feature;
    }


};

double func(int a, double b, float c, long d){
    double sum = a + b + c + d;
    return sum;
}

int main() {
    ThreadPool<2> tp;

    auto res_future = tp.enqueue(func, 0, 0.2, 0.2f, 3);

    std::cout << res_future.get() << std::endl;

    //tp.join();

    return 0;
}

