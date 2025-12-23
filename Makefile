CXX = g++
CXXFLAGS = -std=c++17 -Wall -I/usr/include/postgresql
LDFLAGS = -lpqxx -lpq

all: cinema_app

cinema_app: cinema_db.cpp
	$(CXX) $(CXXFLAGS) -o cinema_app cinema_db.cpp $(LDFLAGS)

clean:
	rm -f cinema_app

run: cinema_app
	./cinema_app