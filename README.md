tasket
======


Example:
	
	tasket::executor executor;

	source_node<std::string> in(executor, [&](tasket::push_type<std::string>& sink)
	{
		std::ifstream infile("infile.txt");

		while (true)
		{
			std::string str;
			{					
				scoped_oversubscription oversubscribe;

				if (!std::getline(infile, str))
					break;
			}
			sink(str);
		}
	});

	generator_node<std::string, char> transform(executor, [&](tasket::pull_type<std::string>& source, tasket::push_type<char>& sink)
	{
		for (auto str : source)
		{
			for (auto c : str)
			{
				sink(c);
				sink(' ');
			}
		}
	});

	generator_node<std::string, char> out(executor, [&](tasket::pull_type<std::string>& source, tasket::push_type<char>& sink)
	{
		std::ofstream outfile("outfile.txt");

		for (auto c : source)
		{
			scoped_oversubscription oversubscribe;
			outfile << c;
		}
	});

	make_edge(in, transform);
	make_edge(transform, out);

	in.activate();

	executor.wait();