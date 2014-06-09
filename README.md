tasket
======


Example:
	
	tasket::executor executor;

	broadcast_node<void*> end;

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
				continue;

			for (auto c : *str)
			{
				sink(c);
				sink(' ');
			}
		}
		sink(nullptr);
	});

	generator_node<std::string, void*> out(executor, [&](tasket::pull_type<std::optional<std::string>>>& source, tasket::push_type<void*>& sink)
	{
		std::ofstream outfile("outfile.txt");

		for (auto c : source)
		{
			if (!c)
				continue;

			tasket::scoped_oversubscription oversubscribe;
			outfile << *c;
		}
		sink(nullptr);
	});

	make_edge(in, transform);
	make_edge(transform, out);
	make_edge(out, end);

	in.try_put(nullpt, nullptr); // Start

	executor.wait();