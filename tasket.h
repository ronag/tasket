#pragma once

#include "assert.h"

#include <ppl.h>

#include <boost/coroutine/coroutine.hpp>
#include <boost/optional.hpp>

#include <condition_variable>
#include <functional>
#include <list>
#include <mutex>
#include <unordered_map>
#include <queue>

namespace tasket
{
    template<typename T>
    using pull_type = boost::coroutines::pull_coroutine<T>;
    template<typename T>
    using push_type = boost::coroutines::push_coroutine<T>;

    struct scoped_oversubscription
    {
        scoped_oversubscription()
        {
            concurrency::Context::Oversubscribe(true);
        }

        ~scoped_oversubscription()
        {
            concurrency::Context::Oversubscribe(false);
        }
    };

    class executor
    {
    public:

        executor()
        {
        }

        void run(std::function<void()> func)
        {
            task_group_.run(std::move(func));
        }

        void wait_for_all()
        {
            run([=]
            {              
                std::unique_lock<std::mutex> wait_lock(wait_mutex_);
                wait_cond_.wait(wait_lock, [this] { return wait_count_ == 0; }); // NOTE: Cooperative block.
            });
            task_group_.wait();
        }

        void increment_wait_count()
        {
            ++wait_count_;
        }

        void decrement_wait_count()
        {
            --wait_count_;
            wait_cond_.notify_one();
        }

    private:
        concurrency::task_group     task_group_;
        std::mutex                  wait_mutex_; // NOTE: Safe to use with ConcRT. See http://msdn.microsoft.com/en-us/library/hh874761.aspx.
        std::condition_variable_any wait_cond_; // NOTE: Safe to use with ConcRT. See http://msdn.microsoft.com/en-us/library/hh921467.aspx.
        std::atomic<int>            wait_count_;
    };

    template<typename T>
    struct receiver;

    template<typename T>
    struct sender;

    template<typename T>
    struct receiver
    {
        using input_type = T;
        using predecessor_type = sender<input_type>;

        virtual ~receiver(){}

        virtual bool try_put(input_type& i, predecessor_type* s) = 0;
    };

    template<typename T>
    struct sender
    {
        using output_type = T;
        using successor_type = receiver<output_type>;

        virtual ~sender(){}

        virtual bool try_get(output_type& o, successor_type* r) = 0;

        virtual void register_successor(successor_type& r) = 0;
    };

    template<typename T>
    void make_edge(sender<T>& s, receiver<T>& r)
    {
        s.register_successor(r);
    }

    template<typename T>
    class successor_cache
    {
    public:
        using input_type = T;
        using successor_type = receiver<input_type>;
        using predecessor_type = sender<input_type>;

        successor_cache(predecessor_type* owner)
            : owner_(owner)
        {
        }

        void add(successor_type* r)
        {
            if (r)
                successors_.push_back(r);
        }

        bool try_put(input_type& i)
        {
            for (auto it = successors_.begin(); it != successors_.end(); it = successors_.erase(it))
            {
                ASSERT(*it);

                if ((*it)->try_put(i, owner_))
                    return true;
            }

            return false;
        }
    private:
        std::list<successor_type*> successors_;
        predecessor_type*          owner_;
    };

    template<typename T>
    class predecessor_cache
    {
    public:
        using output_type = T;
        using successor_type = receiver<output_type>;
        using predecessor_type = sender<output_type>;

        predecessor_cache(successor_type* owner)
            : owner_(owner)
        {
        }

        void add(predecessor_type* s)
        {
            if (s)
                predecessors_.push_back(s);
        }

        bool try_get(output_type& o)
        {
            for (auto it = predecessors_.begin(); it != predecessors_.end(); it = predecessors_.erase(it))
            {
                ASSERT(*it);

                if ((*it)->try_get(o, owner_))
                    return true;
            }

            return false;
        }
    private:
        std::list<predecessor_type*> predecessors_;
        successor_type*              owner_;
    };

    template<typename T>
    class broadcast_node final
        : public receiver<T>
        , public sender<T>
    {
    public:

        broadcast_node()
        {
        }

        broadcast_node(const broadcast_node&) = delete;
        broadcast_node(broadcast_node&&) = delete;

        broadcast_node& operator=(const broadcast_node&) = delete;
        broadcast_node& operator=(broadcast_node&&) = delete;

        bool try_put(input_type& i, predecessor_type* s) override
        {
            std::lock_guard<std::mutex> lock(mutex_);

            for (auto successor : successors_)
                successor->try_put(input_type{ i }, nullptr);

            return true;
        }

        bool try_get(output_type& o, successor_type* r) override
        {
            std::lock_guard<std::mutex> lock(mutex_);

            successors_.push_back(r);

            return false;
        }

        void register_successor(successor_type& r) override
        {
            std::lock_guard<std::mutex> lock(mutex_);

            successors_.push_back(&r);
        }
    private:
        std::list<successor_type*> successors_;
        std::mutex                 mutex_;
    };


    template<typename T>
    class overwrite_node final
        : public receiver<T>
        , public sender<T>
    {
    public:

        overwrite_node()
        {
        }

        overwrite_node(const overwrite_node&) = delete;
        overwrite_node(overwrite_node&&) = delete;

        overwrite_node& operator=(const overwrite_node&) = delete;
        overwrite_node& operator=(overwrite_node&&) = delete;

        bool try_put(input_type& i, predecessor_type* s) override
        {
            std::lock_guard<std::mutex> lock(mutex_);

            for (auto successor : successors_)
                successor->try_put(input_type{ i }, this);

            value_ = std::move(i);

            return true;
        }

        bool try_get(output_type& o, successor_type* r) override
        {
            std::lock_guard<std::mutex> lock(mutex_);

            if (!value_)
            {
                successors_.push_back(r);

                return false;
            }

            o = *value_;

            return true;
        }

        void register_successor(successor_type& r) override
        {
            std::lock_guard<std::mutex> lock(mutex_);

            successors_.push_back(&r);
        }
    private:
        std::list<successor_type*>      successors_;
        boost::optional<input_type>     value_;
        std::mutex                      mutex_;
    };

    template<typename T>
    class queue_node final
        : public receiver<T>
        , public sender<T>
    {
    public:

        queue_node()
            : successors_(this)
        {
        }

        queue_node(const queue_node&) = delete;
        queue_node(queue_node&&) = delete;

        queue_node& operator=(const queue_node&) = delete;
        queue_node& operator=(queue_node&&) = delete;

        bool try_put(input_type& i, predecessor_type* s) override
        {
            std::lock_guard<std::mutex> lock(mutex_);

            if (!successors_.try_put(i))
                queue_.push(std::move(i));
            else
                ASSERT(queue_.empty());

            return true;
        }

        bool try_get(output_type& o, successor_type* r) override
        {
            std::lock_guard<std::mutex> lock(mutex_);

            if (queue_.empty())
            {
                successors_.add(r);

                return false;
            }

            o = std::move(queue_.front());
            queue_.pop();

            return true;
        }

        void register_successor(successor_type& r) override
        {
            std::lock_guard<std::mutex> lock(mutex_);

            successors_.add(&r);
        }
    private:
        successor_cache<output_type> successors_;
        std::queue<input_type>       queue_;
        std::mutex                   mutex_;
    };

    template<typename T>
    class filter_node final
        : public receiver<T>
        , public sender<T>
    {
    public:
        using predicate_type = std::function<bool(const T&)>;

        template<typename Predicate>
        filter_node(Predicate&& predicate)
            : successors_(this)
            , predecessors_(this)
            , predicate_(std::forward<Predicate>(predicate))
        {
        }

        filter_node(const filter_node&) = delete;
        filter_node(filter_node&&) = delete;

        filter_node& operator=(const filter_node&) = delete;
        filter_node& operator=(filter_node&&) = delete;

        bool try_put(input_type& i, predecessor_type* s) override
        {
            std::lock_guard<std::mutex> lock(mutex_);

            if (!predicate_(i))
                return true;

            if (successors_.try_put(i))
                return true;

            predecessors_.add(s);

            return false;
        }

        bool try_get(output_type& o, successor_type* r) override
        {
            std::lock_guard<std::mutex> lock(mutex_);

            output_type o2;
            while (predecessors_.try_get(o2))
            {
                if (predicate_(o2))
                {
                    o = std::move(o2);
                    return true;
                }
            }

            successors_.add(r);

            return false;
        }

        void register_successor(successor_type& r) override
        {
            std::lock_guard<std::mutex> lock(mutex_);

            successors_.add(&r);
        }
    private:
        successor_cache<output_type>    successors_;
        predecessor_cache<input_type>   predecessors_;
        predicate_type                  predicate_;
        std::mutex                      mutex_;
    };

    template<typename Input, typename Output>
    class function_node final
        : public receiver<Input>
        , public sender<Output>
    {
    public:
        using body_type = std::function<output_type(input_type&)>;

        template<typename Body>
        function_node(executor& executor, Body&& body)
            : executor_(executor)
            , successors_(this)
            , predecessors_(this)
            , body_(std::forward<Body>(body))
            , active_(false)
        {
        }

        function_node(const function_node&) = delete;
        function_node(function_node&&) = delete;

        function_node& operator=(const function_node&) = delete;
        function_node& operator=(function_node&&) = delete;

        bool try_put(input_type& i, predecessor_type* s) override
        {
            std::lock_guard<std::mutex> lock(mutex_);

            if (active_ || value_)
            {
                predecessors_.add(s);

                return false;
            }

            spawn_put(std::move(i));

            return true;
        }

        bool try_get(output_type& o, successor_type* r) override
        {
            std::lock_guard<std::mutex> lock(mutex_);

            if (!value_)
            {
                successors_.add(r);

                return false;
            }

            o = std::move(*value_);
            value_.reset();

            if (!active_)
                spawn_get();

            return true;
        }

        void register_successor(successor_type& r) override
        {
            std::lock_guard<std::mutex> lock(mutex_);

            successors_.add(&r);
        }
    private:

        void spawn_get()
        {
            ASSERT(!value_);
            ASSERT(!active_);

            input_type i;
            if (predecessors_.try_get(i))
                spawn_put(std::move(i));
        }

        void spawn_put(input_type& i)
        {
            ASSERT(!value_);
            ASSERT(!active_);

            active_ = true;
            executor_.run([=]() mutable
            {
                ASSERT(!value_);

                auto o = body_(i);

                std::lock_guard<std::mutex> lock(mutex_);

                active_ = false;

                if (successors_.try_put(o))
                    spawn_get();
                else
                    value_ = std::move(o);
            });
        }

        executor&                       executor_;
        successor_cache<output_type>    successors_;
        predecessor_cache<input_type>   predecessors_;
        body_type                       body_;
        bool                            active_;
        boost::optional<output_type>    value_;
        std::mutex                      mutex_;
    };

    template<typename Input, typename Output, typename Generator = std::function<void(pull_type<Input>&, push_type<Output>&)>>
    class generator_node final
        : public receiver<Input>
        , public sender<Output>
    {
    public:

        using source_type = pull_type<input_type>;
        using sink_type = push_type<output_type>;
        using generator_type = Generator;

        template<typename Generator>
        generator_node(executor& executor, Generator&& generator)
            : executor_(executor)
            , successors_(this)
            , predecessors_(this)
            , output_([&](pull_type<output_type>& source)
        {
            for (auto o : source)
            {
                std::unique_lock<std::mutex> lock(mutex_);

                if (!successors_.try_put(o))
                    value_ = std::move(o);

                while (value_)
                    cond_.wait(lock); // NOTE: Cooperative block.
            }
        })
            , generator_(std::bind(std::forward<Generator>(generator), std::placeholders::_1, std::ref(output_)))
            , input_([&](pull_type<input_type>& source)
        {
            for (auto i : source)
            {
                while (true)
                {
                    generator_(i);

                    std::unique_lock<std::mutex> lock(mutex_);

                    active_ = false;

                    if (!predecessors_.try_get(i))
                        break;

                    active_ = true;
                }
            }
        })
            , active_(false)
        {
        }

        generator_node(const generator_node&) = delete;
        generator_node(generator_node&&) = delete;

        generator_node& operator=(const generator_node&) = delete;
        generator_node& operator=(generator_node&&) = delete;

        bool try_put(input_type& i, predecessor_type* s) override
        {
            std::lock_guard<std::mutex> lock(mutex_);

            if (active_ || value_)
            {
                predecessors_.add(s);

                return false;
            }

            active_ = true;
            executor_.run([=]
            {
                input_(i);
            });

            return true;
        }

        bool try_get(output_type& o, successor_type* r) override
        {
            std::lock_guard<std::mutex> lock(mutex_);

            if (!value_)
            {
                successors_.add(r);

                return false;
            }

            o = std::move(*value_);
            value_.reset();
            cond_.notify_one();

            return true;
        }

        void register_successor(successor_type& r) override
        {
            std::lock_guard<std::mutex> lock(mutex_);

            successors_.add(&r);
        }
    private:

        executor&                       executor_;
        successor_cache<output_type>    successors_;
        predecessor_cache<input_type>   predecessors_;

        bool                            active_;
        push_type<output_type>          output_;
        push_type<input_type>           generator_;
        push_type<input_type>           input_;

        boost::optional<output_type>    value_;
        std::condition_variable         cond_;
        std::mutex                      mutex_;
    };

    template<typename T>
    class source_node final
        : public sender<T>
    {
        using generator_node_type = generator_node<void*, output_type>;
    public:
        using sink_type = typename generator_node_type::sink_type;
        using body_type = std::function<void(sink_type&)>;

        template<typename Generator>
        source_node(executor& executor, Generator&& generator)
            : generator_node_(executor, [=](generator_node_type::source_type& source, generator_node_type::sink_type& sink)
        {
            generator(sink);
        })
        {
        }

        source_node(const source_node&) = delete;
        source_node(source_node&&) = delete;

        source_node& operator=(const source_node&) = delete;
        source_node& operator=(source_node&&) = delete;

        void activate()
        {
            void* dummy = nullptr;
            generator_node_.try_put(dummy, nullptr);
        }

        bool try_get(output_type& o, successor_type* r)
        {
            return generator_node_.try_get(o, r);
        }

        void register_successor(successor_type& r)
        {
            generator_node_.register_successor(r);
        }

    private:
        generator_node_type generator_node_;
    };
}