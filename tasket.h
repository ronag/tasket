#pragma once

#include "assert.h"

#include <boost/optional.hpp>
#include <boost/coroutine/coroutine.hpp>

#include <assert.h>
#include <condition_variable>
#include <functional>
#include <list>
#include <mutex>
#include <queue>

#if !defined(TASKET_CONCRT) && !defined(TASKET_CONCRT)
#define TASKET_CONCRT;
#endif

namespace tasket
{

#ifdef TASKET_CONCRT

#include <ppl.h>

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
                // NOTE: Cooperative block.
                std::unique_lock<std::mutex> wait_lock(wait_mutex_);
                wait_cond_.wait(wait_lock, [this] { return wait_count_ == 0; }); 
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
    
#elif TASKET_TBB

#include <tbb/flow_graph.h>

    struct scoped_oversubscription
    {
        scoped_oversubscription()
        {
            // NOT IMPLEMENTED
        }

        ~scoped_oversubscription()
        {
            // NOT IMPLEMENTED
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
            g_.run(std::move(func));
        }

        void wait_for_all()
        {
            g_.wait_for_all();
        }

        void increment_wait_count()
        {
            g_.increment_wait_count();
        }

        void decrement_wait_count()
        {
            g_.decrement_wait_count();
        }

    private:
        tbb::flow_graph g_;
    };

#endif
        
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

        virtual bool try_put(input_type& i, predecessor_type* s = nullptr) = 0;
    };

    template<typename T>
    struct sender
    {
        using output_type = T;
        using successor_type = receiver<output_type>;

        virtual ~sender(){}

        virtual bool try_get(output_type& o, successor_type* r = nullptr) = 0;
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

        successor_cache()
            : owner_(nullptr)
        {
        }

        void set_owner(predecessor_type* s)
        {
            owner_ = s;
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
                assert(*it);

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

        predecessor_cache()
            : owner_(nullptr)
        {
        }

        void set_owner(successor_type* r)
        {
            owner_ = r;
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
                assert(*it);

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

        bool try_put(input_type& i, predecessor_type* s = nullptr) override
        {
            std::lock_guard<std::mutex> lock(mutex_);

            for (auto successor : successors_)
                successor->try_put(input_type{ i });

            return true;
        }

        bool try_get(output_type& o, successor_type* r = nullptr) override
        {
            std::lock_guard<std::mutex> lock(mutex_);

            successors_.push_back(r);

            return false;
        }

        void register_successor(successor_type& r) override
        {
            boost::lock_guard<std::mutex> lock(mutex_);

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

        bool try_put(input_type& i, predecessor_type* s = nullptr) override
        {
            std::lock_guard<std::mutex> lock(mutex_);

            for (auto successor : successors_)
                successor->try_put(input_type{ i }, this);

            value_ = std::move(i);

            return true;
        }

        bool try_get(output_type& o, successor_type* r = nullptr) override
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
        std::list<successor_type*>  successors_;
        boost::optional<input_type> value_;
        std::mutex                  mutex_;
    };

    template<typename T>
    class queue_node final
        : public receiver<T>
        , public sender<T>
    {
    public:

        queue_node()
        {
            successors_.set_owner(this);
        }

        queue_node(const queue_node&) = delete;
        queue_node(queue_node&&) = delete;

        queue_node& operator=(const queue_node&) = delete;
        queue_node& operator=(queue_node&&) = delete;

        bool try_put(input_type& i, predecessor_type* s = nullptr) override
        {
            std::lock_guard<std::mutex> lock(mutex_);

            if (!successors_.try_put(i))
                queue_.push(std::move(i));
            else
                assert(queue_.empty());

            return true;
        }

        bool try_get(output_type& o, successor_type* r = nullptr) override
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
            boost::lock_guard<std::mutex> lock(mutex_);

            successors_.add(&r);
        }
    private:
        successor_cache<output_type> successors_;
        std::queue<input_type>       queue_;
        std::mutex                   mutex_;
    };

    template<typename T>
    class source_node final
        : public sender<T>
    {
    public:
        using body_type = std::function<bool(output_type&)>;

        template<typename Body>
        source_node(executor& executor, Body&& body)
            : executor_(executor)
            , body_(std::move(body))
        {
            successors_.set_owner(this);
        }

        source_node(const source_node&) = delete;
        source_node(source_node&&) = delete;

        source_node& operator=(const source_node&) = delete;
        source_node& operator=(source_node&&) = delete;

        void activate()
        {
            spawn_put();
        }

        bool try_get(output_type& o, successor_type* r = nullptr) override
        {
            std::lock_guard<std::mutex> lock(mutex_);

            if (!value_)
            {
                successors_.add(r);

                return false;
            }

            o = std::move(*value_);
            value_.reset();

            spawn_put();

            return true;
        }

        void register_successor(successor_type& r) override
        {
            std::lock_guard<std::mutex> lock(mutex_);

            successors_.add(&r);
        }

    private:
        void spawn_put()
        {
            executor_.run([=]
            {
                output_type o;
                if (!body_(o))
                    return;

                boost::lock_guard<std::mutex> lock(mutex_);

                if (!successors_.try_put(o))
                    value_ = std::move(o);
                else
                    spawn_put();
            });
        }

        executor&                    executor_;
        successor_cache<output_type> successors_;
        body_type                    body_;
        boost::optional<output_type> value_;
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
            : predicate_(std::forward<Predicate>(predicate))
        {
            successors_.set_owner(this);
            predecessors_.set_owner(this);
        }

        filter_node(const filter_node&) = delete;
        filter_node(filter_node&&) = delete;

        filter_node& operator=(const filter_node&) = delete;
        filter_node& operator=(filter_node&&) = delete;

        bool try_put(input_type& i, predecessor_type* s = nullptr) override
        {
            std::lock_guard<std::mutex> lock(mutex_);

            if (!predicate_(i))
                return true;

            if (successors_.try_put(i))
                return true;

            predecessors_.add(s);

            return false;
        }

        bool try_get(output_type& o, successor_type* r = nullptr) override
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
    class generator_node final
        : public receiver<Input>
        , public sender<Output>
    {
    public:
        
        using source_type    = boost::coroutines::pull_coroutine<output_type>;
        using sink_type      = boost::coroutines::push_coroutine<output_type>;
        using body_type      = std::function<void(sink_type&)>;
        using generator_type = std::function<body_type(input_type&)>;

        template<typename Generator>
        generator_node(executor& executor, Generator&& generator)
            : executor_(executor)
            , generator_(std::forward<Generator>(generator))
            , source_([](sink_type& sink){})
            , active_(false)
        {
            successors_.set_owner(this);
            predecessors_.set_owner(this);
        }

        generator_node(const generator_node&) = delete;
        generator_node(generator_node&&) = delete;

        generator_node& operator=(const generator_node&) = delete;
        generator_node& operator=(generator_node&&) = delete;
        
        bool try_put(input_type& i, predecessor_type* s = nullptr) override
        {
            boost::lock_guard<std::mutex> lock(mutex_);

            if (active_ || value_)
            {
                predecessors_.add(s);

                return false;
            }
            
            spawn_put(i);

            return true;
        }

        bool try_get(output_type& o, successor_type* r = nullptr) override
        {
            boost::lock_guard<std::mutex> lock(mutex_);

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
            boost::lock_guard<std::mutex> lock(mutex_);

            successors_.add(&r);
        }
    private:
        
        void spawn_get()
        {
            ASSERT(!active_);

            input_type i;
            if (predecessors_.try_get(i))
                spawn_put(i);
        }

        void spawn_put(input_type& i)
        {
            ASSERT(!source_);

            source_ = source_type(generator_(std::move(i)));
            spawn();
        }

        void spawn()
        {
            active_ = true;
            executor_.run([=]() mutable
            {
                boost::optional<output_type> o_opt;

                if (source_)
                {
                    o_opt = std::move(source_.get());
                    source_();
                }

                boost::lock_guard<std::mutex> lock(mutex_);
                
                active_ = false;

                if (!o_opt)
                    spawn_get();
                else if (successors_.try_put(*o_opt))
                    spawn();                
                else
                    value_ = std::move(*o_opt);
            });
        }

        executor&                       executor_;
        successor_cache<output_type>    successors_;
        predecessor_cache<input_type>   predecessors_;
        bool                            active_;
        source_type                     source_;
        generator_type                  generator_;
        boost::optional<output_type>    value_;
        std::mutex                      mutex_;
    };
}