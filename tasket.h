#pragma once

#include "assert.h"

#include <tbb/flow_graph.h>

#include <boost/optional.hpp>
#include <boost/thread/condition_variable.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/thread/lock_guard.hpp>

#include <functional>
#include <list>
#include <queue>

namespace task
{
	using default_executor_type = tbb::flow::graph; // TODO: Change to task-group.

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
				ASSERT(*it);

				if ((*it)->try_put(i, owner_))
					return true;
			}

			return false;
		}
	private:
		std::list<successor_type*> successors_;
		predecessor_type*          owner_ = nullptr;
	};

	template<typename T>
	class predecessor_cache
	{
	public:
		using output_type = T;
		using successor_type = receiver<output_type>;
		using predecessor_type = sender<output_type>;

		predecessor_cache()
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
				ASSERT(*it);

				if ((*it)->try_get(o, owner_))
					return true;
			}

			return false;
		}
	private:
		std::list<predecessor_type*> predecessors_;
		successor_type*              owner_ = nullptr;
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
			boost::lock_guard<boost::mutex> lock(mutex_);

			for (auto successor : successors_)
				successor->try_put(input_type{ i });

			return true;
		}

		bool try_get(output_type& o, successor_type* r = nullptr) override
		{
			boost::lock_guard<boost::mutex> lock(mutex_);

			successors_.push_back(r);

			return false;
		}

		void register_successor(successor_type& r) override
		{
			boost::lock_guard<boost::mutex> lock(mutex_);

			successors_.push_back(&r);
		}
	private:
		std::list<successor_type*> successors_;
		boost::mutex               mutex_;
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
			boost::lock_guard<boost::mutex> lock(mutex_);

			for (auto successor : successors_)
				successor->try_put(input_type{ i }, this);

			value_ = std::move(i);

			return true;
		}

		bool try_get(output_type& o, successor_type* r = nullptr) override
		{
			boost::lock_guard<boost::mutex> lock(mutex_);

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
			boost::lock_guard<boost::mutex> lock(mutex_);

			successors_.push_back(&r);
		}
	private:
		std::list<successor_type*>  successors_;
		boost::optional<input_type> value_;
		boost::mutex                mutex_;
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
			boost::lock_guard<boost::mutex> lock(mutex_);

			if (!successors_.try_put(i))
				queue_.push(std::move(i));
			else
				ASSERT(queue_.empty());

			return true;
		}

		bool try_get(output_type& o, successor_type* r = nullptr) override
		{
			boost::lock_guard<boost::mutex> lock(mutex_);

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
			boost::lock_guard<boost::mutex> lock(mutex_);

			successors_.add(&r);
		}
	private:
		successor_cache<output_type> successors_;
		std::queue<input_type>	   queue_;
		boost::mutex				   mutex_;
	};

	template<typename T, typename E = default_executor_type>
	class source_node final
		: public sender<T>
	{
	public:
		using body_type = std::function<bool(output_type&)>;

		template<typename Body>
		source_node(E& executor, Body&& body)
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
			boost::lock_guard<boost::mutex> lock(mutex_);

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
			boost::lock_guard<boost::mutex> lock(mutex_);

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

				boost::lock_guard<boost::mutex> lock(mutex_);

				if (!successors_.try_put(o))
					value_ = std::move(o);
				else
					spawn_put();
			});
		}

		E&							 executor_;
		successor_cache<output_type> successors_;
		body_type					 body_;
		boost::optional<output_type> value_;
		boost::mutex				 mutex_;
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
			boost::lock_guard<boost::mutex> lock(mutex_);

			if (!predicate_(i))
				return true;

			if (successors_.try_put(i))
				return true;

			predecessors_.add(s);

			return false;
		}

		bool try_get(output_type& o, successor_type* r = nullptr) override
		{
			boost::lock_guard<boost::mutex> lock(mutex_);

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
			boost::lock_guard<boost::mutex> lock(mutex_);

			successors_.add(&r);
		}
	private:

		successor_cache<output_type>	successors_;
		predecessor_cache<input_type>	predecessors_;
		predicate_type					predicate_;
		boost::mutex					mutex_;
	};

	template<typename Input, typename Output, typename E = default_executor_type>
	class function_node final
		: public receiver<Input>
		, public sender<Output>
	{
	public:
		using body_type = std::function<output_type(input_type&)>;

		template<typename Body>
		function_node(E& executor, Body&& body)
			: executor_(executor)
			, body_(std::forward<Body>(body))
			, active_(false)
		{
			successors_.set_owner(this);
			predecessors_.set_owner(this);
		}

		function_node(const function_node&) = delete;
		function_node(function_node&&) = delete;

		function_node& operator=(const function_node&) = delete;
		function_node& operator=(function_node&&) = delete;

		bool try_put(input_type& i, predecessor_type* s = nullptr) override
		{
			boost::lock_guard<boost::mutex> lock(mutex_);

			if (active_)
			{
				predecessors_.add(s);

				return false;
			}

			active_ = true;

			spawn_put(std::move(i));

			return true;
		}

		bool try_get(output_type& o, successor_type* r = nullptr) override
		{
			boost::lock_guard<boost::mutex> lock(mutex_);

			if (!value_)
			{
				successors_.add(r);

				return false;
			}

			o = std::move(*value_);
			value_.reset();

			spawn();

			return true;
		}

		void register_successor(successor_type& r) override
		{
			boost::lock_guard<boost::mutex> lock(mutex_);

			successors_.add(&r);
		}
	private:

		void spawn_put(input_type& i)
		{
			ASSERT(active_);
			executor_.run([=]() mutable
			{
				auto o = body_(i);

				boost::lock_guard<boost::mutex> lock(mutex_);

				if (successors_.try_put(o))
					spawn();
				else
					value_ = std::move(o);
			});
		}

		void spawn()
		{
			ASSERT(active_);

			input_type i;
			if (predecessors_.try_get(i))
				spawn_put(std::move(i));
			else
				active_ = false;
		}


		E&								executor_;
		successor_cache<output_type>	successors_;
		predecessor_cache<input_type>	predecessors_;
		body_type						body_;
		bool							active_;
		boost::optional<output_type>	value_;
		boost::mutex					mutex_;
	};


	template<typename Input, typename Output, typename E = default_executor_type>
	class generator_node final
		: public receiver<Input>
		, public sender<Output>
	{
	public:

		using body_type = std::function<bool(output_type&)>;
		using generator_type = std::function<body_type(input_type&)>;

		template<typename Generator>
		generator_node(E& executor, Generator&& generator)
			: executor_(executor)
			, generator_(std::forward<Generator>(generator))
			, body_(nullptr)
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
			boost::lock_guard<boost::mutex> lock(mutex_);

			predecessors_.add(s);

			if (!active_)
				spawn();

			return false;
		}

		bool try_get(output_type& o, successor_type* r = nullptr) override
		{
			boost::lock_guard<boost::mutex> lock(mutex_);

			if (!value_)
			{
				successors_.add(r);

				return false;
			}

			o = std::move(*value_);
			value_.reset();

			spawn();

			return true;
		}

		void register_successor(successor_type& r) override
		{
			boost::lock_guard<boost::mutex> lock(mutex_);

			successors_.add(&r);
		}
	private:

		void spawn()
		{
			active_ = true;
			executor_.run([=]() mutable
			{
				output_type o;
				auto result = body_ && body_(o);

				boost::lock_guard<boost::mutex> lock(mutex_);

				if (!result)
					get_and_spawn();
				else if (successors_.try_put(o))
					spawn();
				else
					value_ = std::move(o);
			});
		}

		void get_and_spawn()
		{
			ASSERT(active_);

			input_type i;
			if (predecessors_.try_get(i))
			{
				body_ = generator_(i);
				spawn();
			}
			else
			{
				body_ = nullptr;
				active_ = false;
			}
		}

		E&								executor_;
		successor_cache<output_type>	successors_;
		predecessor_cache<input_type>	predecessors_;
		bool							active_;
		body_type						body_;
		generator_type					generator_;
		boost::optional<output_type>	value_;
		boost::mutex					mutex_;
	};
}