tasket
======


Example:
	
	tasket::executor executor;

	generator_node<void*, std::string> in(executor, [&](tasket::pull_type<void*>& source, tasket::push_type<std::optional<std::string>>& sink)
	{
		std::ifstream infile("infile.txt");

		while (true)
		{
			std::string str;
			{					
				tasket::scoped_oversubscription oversubscribe;
				if (!std::getline(infile, str))
					break;
			}
			sink(str);
		}
		sink(nullptr);
	});

	generator_node<std::optional<std::string>>, char> transform(executor, [&](tasket::pull_type<std::optional<std::string>>>& source, tasket::push_type<char>& sink)
	{
		for (auto str : source)
		{
			if (!str)
			{
				sink(nullptr);
				break;
			}

			for (auto c : *str)
			{
				sink(c);
				sink(' ');
			}
		}
	});

	generator_node<std::string, void*> out(executor, [&](tasket::pull_type<std::optional<std::string>>>& source, tasket::push_type<void*>& sink)
	{
		std::ofstream outfile("outfile.txt");

		for (auto c : source)
		{
			if (!c)
			{
				sink(nullptr);
				break;
			}

			tasket::scoped_oversubscription oversubscribe;
			outfile << *c;
		}
	});

	make_edge(in, transform);
	make_edge(transform, out);

	in.try_put(nullpt, nullptr); // Start

	executor.wait();