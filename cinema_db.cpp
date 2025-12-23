#include <iostream>
#include <string>
#include <iomanip>
#include <pqxx/pqxx>
#include <vector>
#include <stdexcept>

class CinemaDatabase {
private:
    pqxx::connection* conn;
    
public:
    CinemaDatabase(const std::string& connection_string) {
        try {
            conn = new pqxx::connection(connection_string);
            if (conn->is_open()) {
                std::cout << "Connected to database successfully!" << std::endl;
            } else {
                throw std::runtime_error("Failed to open database");
            }
        } catch (const std::exception &e) {
            std::cerr << "Database connection error: " << e.what() << std::endl;
            throw;
        }
    }
    
    ~CinemaDatabase() {
        if (conn) {
            conn->close();
            delete conn;
        }
    }
    
    // 1. Показать тестовые данные
    void showTestData() {
        try {
            pqxx::work txn(*conn);
            
            std::cout << "\n=== Test Data Overview ===\n" << std::endl;
            
            // 1. Режиссеры
            std::cout << "1. Directors (режиссеры):" << std::endl;
            pqxx::result directors = txn.exec("SELECT director_id, first_name, last_name, nationality FROM directors ORDER BY director_id");
            std::cout << std::left << std::setw(5) << "ID" 
                      << std::setw(15) << "First Name" 
                      << std::setw(15) << "Last Name" 
                      << std::setw(15) << "Nationality" << std::endl;
            std::cout << std::string(50, '-') << std::endl;
            for (const auto& row : directors) {
                std::cout << std::left << std::setw(5) << row[0].c_str()
                          << std::setw(15) << row[1].c_str()
                          << std::setw(15) << row[2].c_str()
                          << std::setw(15) << row[3].c_str() << std::endl;
            }
            std::cout << std::endl;
            
            // 2. Актеры
            std::cout << "2. Actors (актеры):" << std::endl;
            pqxx::result actors = txn.exec("SELECT actor_id, first_name, last_name, nationality, is_oscar_winner FROM actors ORDER BY actor_id");
            std::cout << std::left << std::setw(5) << "ID" 
                      << std::setw(15) << "First Name" 
                      << std::setw(15) << "Last Name" 
                      << std::setw(15) << "Nationality" 
                      << std::setw(12) << "Oscar Winner" << std::endl;
            std::cout << std::string(62, '-') << std::endl;
            for (const auto& row : actors) {
                std::string oscar = (row[4].c_str()[0] == 't') ? "Yes" : "No";
                std::cout << std::left << std::setw(5) << row[0].c_str()
                          << std::setw(15) << row[1].c_str()
                          << std::setw(15) << row[2].c_str()
                          << std::setw(15) << row[3].c_str()
                          << std::setw(12) << oscar << std::endl;
            }
            std::cout << std::endl;
            
            // 3. Фильмы
            std::cout << "3. Films (фильмы):" << std::endl;
            pqxx::result films = txn.exec(
                "SELECT f.film_id, f.title, f.release_year, f.duration_minutes, "
                "f.budget, f.box_office, d.first_name || ' ' || d.last_name as director "
                "FROM films f "
                "JOIN directors d ON f.director_id = d.director_id "
                "ORDER BY f.film_id"
            );
            std::cout << std::left << std::setw(5) << "ID" 
                      << std::setw(30) << "Title" 
                      << std::setw(8) << "Year" 
                      << std::setw(12) << "Duration" 
                      << std::setw(15) << "Budget" 
                      << std::setw(15) << "Box Office" 
                      << std::setw(20) << "Director" << std::endl;
            std::cout << std::string(105, '-') << std::endl;
            for (const auto& row : films) {
                double budget = std::stod(row[4].c_str());
                double box_office = std::stod(row[5].c_str());
                std::cout << std::left << std::setw(5) << row[0].c_str()
                          << std::setw(30) << row[1].c_str()
                          << std::setw(8) << row[2].c_str()
                          << std::setw(12) << std::string(row[3].c_str()) + " min"
                          << std::setw(15) << std::fixed << std::setprecision(2) << "$" << budget/1000000 << "M"
                          << std::setw(15) << "$" << box_office/1000000 << "M"
                          << std::setw(20) << row[6].c_str() << std::endl;
            }
            std::cout << std::endl;
            
            // 4. Жанры
            std::cout << "4. Genres (жанры):" << std::endl;
            pqxx::result genres = txn.exec("SELECT genre_id, name, description FROM genres ORDER BY genre_id");
            std::cout << std::left << std::setw(5) << "ID" 
                      << std::setw(15) << "Name" 
                      << std::setw(30) << "Description" << std::endl;
            std::cout << std::string(50, '-') << std::endl;
            for (const auto& row : genres) {
                std::cout << std::left << std::setw(5) << row[0].c_str()
                          << std::setw(15) << row[1].c_str()
                          << std::setw(30) << row[2].c_str() << std::endl;
            }
            std::cout << std::endl;
            
            // 5. Связи фильмов и актеров
            std::cout << "5. Film Roles (роли актеров в фильмах):" << std::endl;
            pqxx::result roles = txn.exec(
                "SELECT f.title, a.first_name || ' ' || a.last_name as actor, "
                "fr.character_name, fr.is_main_role "
                "FROM film_roles fr "
                "JOIN films f ON fr.film_id = f.film_id "
                "JOIN actors a ON fr.actor_id = a.actor_id "
                "ORDER BY f.title, fr.is_main_role DESC"
            );
            std::cout << std::left << std::setw(30) << "Film" 
                      << std::setw(25) << "Actor" 
                      << std::setw(25) << "Character" 
                      << std::setw(12) << "Main Role" << std::endl;
            std::cout << std::string(92, '-') << std::endl;
            if (roles.empty()) {
                std::cout << "No film roles found." << std::endl;
            } else {
                for (const auto& row : roles) {
                    std::string is_main = (row[3].c_str()[0] == 't') ? "Yes" : "No";
                    std::cout << std::left << std::setw(30) << row[0].c_str()
                              << std::setw(25) << row[1].c_str()
                              << std::setw(25) << row[2].c_str()
                              << std::setw(12) << is_main << std::endl;
                }
            }
            std::cout << std::endl;
            
            // 6. Отзывы
            std::cout << "6. Reviews (отзывы):" << std::endl;
            pqxx::result reviews = txn.exec(
                "SELECT f.title, r.reviewer_name, r.rating, r.comment "
                "FROM reviews r "
                "JOIN films f ON r.film_id = f.film_id "
                "ORDER BY f.title, r.rating DESC"
            );
            std::cout << std::left << std::setw(30) << "Film" 
                      << std::setw(15) << "Reviewer" 
                      << std::setw(10) << "Rating" 
                      << std::setw(30) << "Comment" << std::endl;
            std::cout << std::string(85, '-') << std::endl;
            if (reviews.empty()) {
                std::cout << "No reviews found." << std::endl;
            } else {
                for (const auto& row : reviews) {
                    std::cout << std::left << std::setw(30) << row[0].c_str()
                              << std::setw(15) << row[1].c_str()
                              << std::setw(10) << row[2].c_str()
                              << std::setw(30) << row[3].c_str() << std::endl;
                }
            }
            std::cout << std::endl;
            
            // 7. Сводная статистика
            std::cout << "7. Summary Statistics (сводная статистика):" << std::endl;
            pqxx::result stats = txn.exec(
                "SELECT 'Directors' as category, COUNT(*)::text as count FROM directors "
                "UNION ALL SELECT 'Actors', COUNT(*)::text FROM actors "
                "UNION ALL SELECT 'Films', COUNT(*)::text FROM films "
                "UNION ALL SELECT 'Genres', COUNT(*)::text FROM genres "
                "UNION ALL SELECT 'Film Roles', COUNT(*)::text FROM film_roles "
                "UNION ALL SELECT 'Reviews', COUNT(*)::text FROM reviews "
                "UNION ALL SELECT 'Awards', COUNT(*)::text FROM awards "
                "ORDER BY category"
            );
            std::cout << std::left << std::setw(15) << "Category" 
                      << std::setw(10) << "Count" << std::endl;
            std::cout << std::string(25, '-') << std::endl;
            for (const auto& row : stats) {
                std::cout << std::left << std::setw(15) << row[0].c_str()
                          << std::setw(10) << row[1].c_str() << std::endl;
            }
            
        } catch (const std::exception &e) {
            std::cerr << "Error showing test data: " << e.what() << std::endl;
        }
    }
    
    // 2. Поиск фильмов по году выпуска
    void findFilmsByYear(int year) {
        try {
            pqxx::work txn(*conn);
            std::string query = "SELECT f.film_id, f.title, f.release_year, f.duration_minutes, "
                               "d.first_name || ' ' || d.last_name as director "
                               "FROM films f "
                               "LEFT JOIN directors d ON f.director_id = d.director_id "
                               "WHERE f.release_year = $1 "
                               "ORDER BY f.title";
            pqxx::result r = txn.exec_params(query, year);
            
            std::cout << "\n=== Films released in " << year << " ===" << std::endl;
            if (r.empty()) {
                std::cout << "No films found." << std::endl;
                return;
            }
            
            std::cout << std::left << std::setw(5) << "ID" 
                      << std::setw(40) << "Title" 
                      << std::setw(10) << "Duration" 
                      << std::setw(25) << "Director" << std::endl;
            std::cout << std::string(80, '-') << std::endl;
            
            for (const auto& row : r) {
                std::cout << std::left << std::setw(5) << row[0].c_str()
                          << std::setw(40) << row[1].c_str()
                          << std::setw(10) << std::string(row[3].c_str()) + " min"
                          << std::setw(25) << row[4].c_str() << std::endl;
            }
            
            std::cout << "\nTotal films: " << r.size() << std::endl;
            
        } catch (const std::exception &e) {
            std::cerr << "Error searching films: " << e.what() << std::endl;
        }
    }
    
    // 3. Получение статистики по режиссерам
    void getDirectorStatistics() {
        try {
            pqxx::work txn(*conn);
            std::string query = "SELECT d.director_id, d.first_name || ' ' || d.last_name as director_name, "
                               "COUNT(f.film_id) as film_count, "
                               "SUM(f.box_office) as total_box_office, "
                               "AVG(f.box_office) as avg_box_office "
                               "FROM directors d "
                               "LEFT JOIN films f ON d.director_id = f.director_id "
                               "GROUP BY d.director_id, director_name "
                               "HAVING COUNT(f.film_id) > 0 "
                               "ORDER BY total_box_office DESC NULLS LAST";
            
            pqxx::result r = txn.exec(query);
            
            std::cout << "\n=== Director Statistics ===" << std::endl;
            if (r.empty()) {
                std::cout << "No directors found." << std::endl;
                return;
            }
            
            std::cout << std::left << std::setw(25) << "Director" 
                      << std::setw(10) << "Films" 
                      << std::setw(15) << "Total Box Office" 
                      << std::setw(15) << "Average" << std::endl;
            std::cout << std::string(65, '-') << std::endl;
            
            for (const auto& row : r) {
                std::cout << std::left << std::setw(25) << row[1].c_str()
                          << std::setw(10) << row[2].c_str();
                
                if (!row[3].is_null()) {
                    double total = std::stod(row[3].c_str());
                    std::cout << std::setw(15) << std::fixed << std::setprecision(2) 
                            << "$" << total/1000000 << "M";
                } else {
                    std::cout << std::setw(15) << "N/A";
                }
                
                if (!row[4].is_null()) {
                    double avg = std::stod(row[4].c_str());
                    std::cout << std::setw(15) << std::fixed << std::setprecision(2) 
                            << "$" << avg/1000000 << "M" << std::endl;
                } else {
                    std::cout << std::setw(15) << "N/A" << std::endl;
                }
            }
            
        } catch (const std::exception &e) {
            std::cerr << "Error getting statistics: " << e.what() << std::endl;
        }
    }
    
    // 4. Поиск актеров по фильму (исправленная версия)
    void findActorsByFilm(const std::string& film_title) {
        try {
            pqxx::work txn(*conn);
            
            std::string query = "SELECT a.actor_id, a.first_name || ' ' || a.last_name as actor_name, "
                               "fr.character_name, fr.is_main_role "
                               "FROM film_roles fr "
                               "JOIN actors a ON fr.actor_id = a.actor_id "
                               "JOIN films f ON fr.film_id = f.film_id "
                               "WHERE LOWER(f.title) LIKE LOWER('%' || $1 || '%') "
                               "ORDER BY fr.is_main_role DESC, a.last_name";
            
            pqxx::result r = txn.exec_params(query, film_title);
            
            std::cout << "\n=== Actors in films matching \"" << film_title << "\" ===" << std::endl;
            if (r.empty()) {
                std::cout << "No actors found for films matching this title." << std::endl;
                return;
            }
            
            std::cout << std::left << std::setw(25) << "Actor" 
                      << std::setw(25) << "Character" 
                      << std::setw(10) << "Main Role" << std::endl;
            std::cout << std::string(60, '-') << std::endl;
            
            for (const auto& row : r) {
                std::string is_main = (row[3].c_str()[0] == 't') ? "Yes" : "No";
                std::cout << std::left << std::setw(25) << row[1].c_str()
                          << std::setw(25) << row[2].c_str()
                          << std::setw(10) << is_main << std::endl;
            }
            
            std::cout << "\nTotal actors found: " << r.size() << std::endl;
            
        } catch (const std::exception &e) {
            std::cerr << "Error finding actors: " << e.what() << std::endl;
        }
    }
    
    // 5. Получение топ фильмов по кассовым сборам
    void getTopGrossingFilms(int limit = 10) {
        try {
            pqxx::work txn(*conn);
            std::string query = "SELECT f.title, f.release_year, f.box_office, "
                               "d.first_name || ' ' || d.last_name as director, "
                               "ROUND((f.box_office - f.budget) / f.budget * 100, 2) as roi "
                               "FROM films f "
                               "JOIN directors d ON f.director_id = d.director_id "
                               "WHERE f.box_office > 0 AND f.budget > 0 "
                               "ORDER BY f.box_office DESC "
                               "LIMIT $1";
            
            pqxx::result r = txn.exec_params(query, limit);
            
            std::cout << "\n=== Top " << limit << " Grossing Films ===" << std::endl;
            if (r.empty()) {
                std::cout << "No films found." << std::endl;
                return;
            }
            
            std::cout << std::left << std::setw(35) << "Title" 
                      << std::setw(8) << "Year" 
                      << std::setw(15) << "Box Office" 
                      << std::setw(20) << "Director" 
                      << std::setw(10) << "ROI %" << std::endl;
            std::cout << std::string(88, '-') << std::endl;
            
            for (const auto& row : r) {
                double box_office = std::stod(row[2].c_str());
                std::cout << std::left << std::setw(35) << row[0].c_str()
                          << std::setw(8) << row[1].c_str()
                          << std::setw(15) << std::fixed << std::setprecision(2) 
                          << "$" << box_office/1000000 << "M"
                          << std::setw(20) << row[3].c_str()
                          << std::setw(10) << row[4].c_str() << std::endl;
            }
            
        } catch (const std::exception &e) {
            std::cerr << "Error getting top films: " << e.what() << std::endl;
        }
    }
    
    // 6. Добавление нового актера
    void addActor(const std::string& first_name, const std::string& last_name, 
                  const std::string& birth_date, const std::string& nationality, 
                  bool oscar_winner = false) {
        try {
            pqxx::work txn(*conn);
            std::string query = "INSERT INTO actors (first_name, last_name, birth_date, nationality, is_oscar_winner) "
                               "VALUES ($1, $2, $3, $4, $5) RETURNING actor_id";
            pqxx::result r = txn.exec_params(query, first_name, last_name, 
                                            birth_date, nationality, oscar_winner);
            txn.commit();
            std::cout << "Actor added successfully! Actor ID: " << r[0][0].as<int>() << std::endl;
        } catch (const std::exception &e) {
            std::cerr << "Error adding actor: " << e.what() << std::endl;
        }
    }
    
    // 7. Поиск фильмов по жанру
    void findFilmsByGenre(const std::string& genre) {
        try {
            pqxx::work txn(*conn);
            std::string query = "SELECT f.title, f.release_year, f.duration_minutes, "
                               "STRING_AGG(g.name, ', ') as genres "
                               "FROM films f "
                               "JOIN film_genres fg ON f.film_id = fg.film_id "
                               "JOIN genres g ON fg.genre_id = g.genre_id "
                               "WHERE LOWER(g.name) LIKE LOWER('%' || $1 || '%') "
                               "GROUP BY f.film_id, f.title, f.release_year, f.duration_minutes "
                               "ORDER BY f.release_year DESC";
            
            pqxx::result r = txn.exec_params(query, genre);
            
            std::cout << "\n=== Films in genre: " << genre << " ===" << std::endl;
            if (r.empty()) {
                std::cout << "No films found." << std::endl;
                return;
            }
            
            std::cout << std::left << std::setw(35) << "Title" 
                      << std::setw(8) << "Year" 
                      << std::setw(10) << "Duration" 
                      << std::setw(25) << "Genres" << std::endl;
            std::cout << std::string(78, '-') << std::endl;
            
            for (const auto& row : r) {
                std::cout << std::left << std::setw(35) << row[0].c_str()
                          << std::setw(8) << row[1].c_str()
                          << std::setw(10) << std::string(row[2].c_str()) + " min"
                          << std::setw(25) << row[3].c_str() << std::endl;
            }
            
        } catch (const std::exception &e) {
            std::cerr << "Error searching by genre: " << e.what() << std::endl;
        }
    }
    
    // 8. Получение среднего рейтинга фильмов
    void getAverageFilmRatings() {
        try {
            pqxx::work txn(*conn);
            std::string query = "SELECT f.title, "
                               "ROUND(AVG(r.rating), 2) as avg_rating, "
                               "COUNT(r.review_id) as review_count "
                               "FROM films f "
                               "LEFT JOIN reviews r ON f.film_id = r.film_id "
                               "GROUP BY f.film_id, f.title "
                               "HAVING COUNT(r.review_id) >= 1 "
                               "ORDER BY avg_rating DESC";
            
            pqxx::result r = txn.exec(query);
            
            std::cout << "\n=== Average Film Ratings ===" << std::endl;
            if (r.empty()) {
                std::cout << "No ratings found." << std::endl;
                return;
            }
            
            std::cout << std::left << std::setw(35) << "Title" 
                      << std::setw(12) << "Avg Rating" 
                      << std::setw(12) << "Reviews" << std::endl;
            std::cout << std::string(59, '-') << std::endl;
            
            for (const auto& row : r) {
                std::cout << std::left << std::setw(35) << row[0].c_str()
                          << std::setw(12) << row[1].c_str()
                          << std::setw(12) << row[2].c_str() << std::endl;
            }
            
        } catch (const std::exception &e) {
            std::cerr << "Error getting ratings: " << e.what() << std::endl;
        }
    }
    
    // 9. Добавление нового фильма
    void addFilm(const std::string& title, int release_year, int duration, 
                 double budget, double box_office, int director_id) {
        try {
            pqxx::work txn(*conn);
            std::string query = "INSERT INTO films (title, release_year, duration_minutes, budget, box_office, director_id) "
                               "VALUES ($1, $2, $3, $4, $5, $6) RETURNING film_id";
            pqxx::result r = txn.exec_params(query, title, release_year, duration, 
                                             budget, box_office, director_id);
            txn.commit();
            std::cout << "Film added successfully! Film ID: " << r[0][0].as<int>() << std::endl;
        } catch (const std::exception &e) {
            std::cerr << "Error adding film: " << e.what() << std::endl;
        }
    }
    
    // 10. Обновление информации о фильме
    void updateFilmBoxOffice(int film_id, double new_box_office) {
        try {
            pqxx::work txn(*conn);
            std::string query = "UPDATE films SET box_office = $1 WHERE film_id = $2";
            txn.exec_params(query, new_box_office, film_id);
            txn.commit();
            std::cout << "Film box office updated successfully!" << std::endl;
        } catch (const std::exception &e) {
            std::cerr << "Error updating film: " << e.what() << std::endl;
        }
    }
    
    // 11. Метод для демонстрации всех 10 запросов
    void demonstrateAllQueries() {
        std::cout << "\n=== Demonstrating All 10 Required SQL Queries ===" << std::endl;
        
        try {
            pqxx::work txn(*conn);
            
            // Запрос 1: SELECT с JOIN и WHERE
            std::cout << "\n1. Films by director Christopher Nolan:" << std::endl;
            pqxx::result r1 = txn.exec(
                "SELECT f.title, f.release_year, f.budget, f.box_office "
                "FROM films f "
                "JOIN directors d ON f.director_id = d.director_id "
                "WHERE d.first_name = 'Christopher' AND d.last_name = 'Nolan'"
            );
            if (r1.empty()) {
                std::cout << "  No films found." << std::endl;
            } else {
                for (const auto& row : r1) {
                    std::cout << "  " << row[0].c_str() << " (" 
                             << row[1].c_str() << ")" << std::endl;
                }
            }
            
            // Запрос 2: SELECT с агрегатной функцией и GROUP BY
            std::cout << "\n2. Average budget by release year:" << std::endl;
            pqxx::result r2 = txn.exec(
                "SELECT release_year, AVG(budget) as avg_budget, COUNT(*) as film_count "
                "FROM films "
                "GROUP BY release_year "
                "HAVING COUNT(*) > 0 "
                "ORDER BY release_year DESC"
            );
            if (r2.empty()) {
                std::cout << "  No data found." << std::endl;
            } else {
                for (const auto& row : r2) {
                    double avg = std::stod(row[1].c_str());
                    std::cout << "  " << row[0].c_str() << ": $" 
                             << std::fixed << std::setprecision(2) << avg/1000000 
                             << "M (" << row[2].c_str() << " films)" << std::endl;
                }
            }
            
            // Запрос 3: SELECT с подзапросом
            std::cout << "\n3. Films with above average box office:" << std::endl;
            pqxx::result r3 = txn.exec(
                "SELECT title, box_office "
                "FROM films "
                "WHERE box_office > (SELECT AVG(box_office) FROM films) "
                "ORDER BY box_office DESC"
            );
            if (r3.empty()) {
                std::cout << "  No films found." << std::endl;
            } else {
                for (const auto& row : r3) {
                    double box = std::stod(row[1].c_str());
                    std::cout << "  " << row[0].c_str() << ": $" 
                             << std::fixed << std::setprecision(2) << box/1000000 
                             << "M" << std::endl;
                }
            }
            
            // Запрос 4: SELECT с LEFT JOIN
            std::cout << "\n4. All directors with their film count:" << std::endl;
            pqxx::result r4 = txn.exec(
                "SELECT d.first_name || ' ' || d.last_name as director, "
                "COUNT(f.film_id) as film_count "
                "FROM directors d "
                "LEFT JOIN films f ON d.director_id = f.director_id "
                "GROUP BY d.director_id "
                "ORDER BY film_count DESC"
            );
            if (r4.empty()) {
                std::cout << "  No directors found." << std::endl;
            } else {
                for (const auto& row : r4) {
                    std::cout << "  " << row[0].c_str() << ": " 
                             << row[1].c_str() << " films" << std::endl;
                }
            }
            
            // Запрос 5: SELECT с INNER JOIN и ORDER BY
            std::cout << "\n5. Films with their genres:" << std::endl;
            pqxx::result r5 = txn.exec(
                "SELECT f.title, STRING_AGG(g.name, ', ') as genres "
                "FROM films f "
                "JOIN film_genres fg ON f.film_id = fg.film_id "
                "JOIN genres g ON fg.genre_id = g.genre_id "
                "GROUP BY f.film_id, f.title "
                "ORDER BY f.title"
            );
            if (r5.empty()) {
                std::cout << "  No films found." << std::endl;
            } else {
                for (const auto& row : r5) {
                    std::cout << "  " << row[0].c_str() << ": " 
                             << row[1].c_str() << std::endl;
                }
            }
            
            // Запрос 6: SELECT с LIMIT и OFFSET
            std::cout << "\n6. Top 3 highest grossing films:" << std::endl;
            pqxx::result r6 = txn.exec(
                "SELECT title, box_office "
                "FROM films "
                "ORDER BY box_office DESC "
                "LIMIT 3"
            );
            if (r6.empty()) {
                std::cout << "  No films found." << std::endl;
            } else {
                for (int i = 0; i < r6.size(); i++) {
                    double box = std::stod(r6[i][1].c_str());
                    std::cout << "  " << (i+1) << ". " << r6[i][0].c_str() 
                             << ": $" << std::fixed << std::setprecision(2) 
                             << box/1000000 << "M" << std::endl;
                }
            }
            
            // Запрос 7: SELECT с CASE
            std::cout << "\n7. Film profitability analysis:" << std::endl;
            pqxx::result r7 = txn.exec(
                "SELECT title, budget, box_office, "
                "CASE "
                "  WHEN box_office > budget * 5 THEN 'Blockbuster' "
                "  WHEN box_office > budget * 2 THEN 'Successful' "
                "  WHEN box_office > budget THEN 'Profitable' "
                "  ELSE 'Unprofitable' "
                "END as profitability "
                "FROM films "
                "ORDER BY box_office DESC"
            );
            if (r7.empty()) {
                std::cout << "  No films found." << std::endl;
            } else {
                for (const auto& row : r7) {
                    std::cout << "  " << row[0].c_str() << ": " 
                             << row[3].c_str() << std::endl;
                }
            }
            
            // Запрос 8: SELECT с оконной функцией
            std::cout << "\n8. Films ranked within their release year:" << std::endl;
            pqxx::result r8 = txn.exec(
                "SELECT title, release_year, box_office, "
                "RANK() OVER (PARTITION BY release_year ORDER BY box_office DESC) as yearly_rank "
                "FROM films "
                "ORDER BY release_year, yearly_rank"
            );
            if (r8.empty()) {
                std::cout << "  No films found." << std::endl;
            } else {
                for (const auto& row : r8) {
                    std::cout << "  " << row[0].c_str() << " (" 
                             << row[1].c_str() << "): Rank " 
                             << row[3].c_str() << std::endl;
                }
            }
            
            // Запрос 9: SELECT с UNION
            std::cout << "\n9. All people in cinema (directors and actors):" << std::endl;
            pqxx::result r9 = txn.exec(
                "SELECT first_name || ' ' || last_name as name, 'Director' as role "
                "FROM directors "
                "UNION "
                "SELECT first_name || ' ' || last_name as name, 'Actor' as role "
                "FROM actors "
                "ORDER BY name "
                "LIMIT 5"
            );
            if (r9.empty()) {
                std::cout << "  No people found." << std::endl;
            } else {
                for (const auto& row : r9) {
                    std::cout << "  " << row[0].c_str() << " - " 
                             << row[1].c_str() << std::endl;
                }
            }
            
            // Запрос 10: SELECT с EXISTS
            std::cout << "\n10. Directors who have won awards:" << std::endl;
            pqxx::result r10 = txn.exec(
                "SELECT d.first_name || ' ' || d.last_name as director "
                "FROM directors d "
                "WHERE EXISTS ("
                "  SELECT 1 FROM films f "
                "  JOIN film_awards fa ON f.film_id = fa.film_id "
                "  WHERE f.director_id = d.director_id"
                ")"
            );
            if (r10.empty()) {
                std::cout << "  No directors found." << std::endl;
            } else {
                for (const auto& row : r10) {
                    std::cout << "  " << row[0].c_str() << std::endl;
                }
            }
            
        } catch (const std::exception &e) {
            std::cerr << "Error demonstrating queries: " << e.what() << std::endl;
        }
    }
    

    // 13. Статистика по длительности фильмов 
    void filmDurationStatistics() {
        try {
            pqxx::work txn(*conn);
            std::string query = 
                "WITH duration_categories AS ("
                "  SELECT "
                "    f.film_id, "
                "    f.title, "
                "    f.duration_minutes, "
                "    r.rating, "
                "    CASE "
                "      WHEN f.duration_minutes < 100 THEN 'Short (< 100 min)' "
                "      WHEN f.duration_minutes >= 100 AND f.duration_minutes < 200 THEN 'Medium (100-200 min)' "
                "      WHEN f.duration_minutes >= 200 THEN 'Long (≥ 200 min)' "
                "      ELSE 'Unknown' "
                "    END as duration_category "
                "  FROM films f "
                "  LEFT JOIN reviews r ON f.film_id = r.film_id "
                ") "
                "SELECT "
                "  duration_category, "
                "  COUNT(DISTINCT film_id) as film_count, "
                " ROUND(AVG(duration_minutes)::numeric, 1) as avg_duration, "
                " ROUND(AVG(rating)::numeric, 2) as avg_rating, "
                "  MIN(rating) as min_rating, "
                "  MAX(rating) as max_rating, "
                "  STRING_AGG(DISTINCT title, ', ' ORDER BY title) as films "
                "FROM duration_categories "
                "GROUP BY duration_category "
                "HAVING COUNT(DISTINCT film_id) > 0 "
                "ORDER BY "
                "  CASE duration_category "
                "    WHEN 'Short (< 100 min)' THEN 1 "
                "    WHEN 'Medium (100-200 min)' THEN 2 "
                "    WHEN 'Long (≥ 200 min)' THEN 3 "
                "    ELSE 4 "
                "  END";
            
            pqxx::result r = txn.exec(query);
            
            std::cout << "\n=== Film Duration Statistics ===" << std::endl;
            std::cout << "Analysis of film ratings based on duration categories\n" << std::endl;
            
            if (r.empty()) {
                std::cout << "No data found." << std::endl;
                return;
            }
            
            std::cout << std::left << std::setw(25) << "Duration Category" 
                      << std::setw(12) << "Films" 
                      << std::setw(15) << "Avg Duration" 
                      << std::setw(12) << "Avg Rating" 
                      << std::setw(12) << "Min Rating" 
                      << std::setw(12) << "Max Rating" << std::endl;
            std::cout << std::string(88, '-') << std::endl;
            
            double overall_avg_rating = 0;
            int total_films = 0;
            
            for (const auto& row : r) {
                int film_count = std::stoi(row[1].c_str());
                double avg_duration = std::stod(row[2].c_str());
                double avg_rating = row[3].is_null() ? 0 : std::stod(row[3].c_str());
                double min_rating = row[4].is_null() ? 0 : std::stod(row[4].c_str());
                double max_rating = row[5].is_null() ? 0 : std::stod(row[5].c_str());
                
                std::cout << std::left << std::setw(25) << row[0].c_str()
                          << std::setw(12) << film_count
                          << std::setw(15) << std::fixed << std::setprecision(1) << avg_duration << " min"
                          << std::setw(12) << std::fixed << std::setprecision(2) << avg_rating
                          << std::setw(12) << std::fixed << std::setprecision(2) << min_rating
                          << std::setw(12) << std::fixed << std::setprecision(2) << max_rating << std::endl;
                
                overall_avg_rating += avg_rating * film_count;
                total_films += film_count;
            }
            
            // Общая статистика
            if (total_films > 0) {
                overall_avg_rating /= total_films;
                std::cout << std::string(88, '-') << std::endl;
                std::cout << std::left << std::setw(25) << "OVERALL"
                          << std::setw(12) << total_films
                          << std::setw(15) << ""
                          << std::setw(12) << std::fixed << std::setprecision(2) << overall_avg_rating
                          << std::setw(12) << ""
                          << std::setw(12) << "" << std::endl;
            }
            
            std::cout << "\n=== Film List by Category ===" << std::endl;
            for (const auto& row : r) {
                std::string category = row[0].c_str();
                std::string films_list = row[6].c_str();
                
                std::cout << "\n" << category << ":" << std::endl;
                std::cout << "  Films: " << films_list << std::endl;
            }
            
        } catch (const std::exception &e) {
            std::cerr << "Error getting duration statistics: " << e.what() << std::endl;
        }
    }
};


void displayMenu() {
    std::cout << "\n=== Cinema Database Management System ===" << std::endl;
    std::cout << "1. Show test data" << std::endl;
    std::cout << "2. Find films by year" << std::endl;
    std::cout << "3. Get director statistics" << std::endl;
    std::cout << "4. Find actors by film" << std::endl;
    std::cout << "5. Get top grossing films" << std::endl;
    std::cout << "6. Find films by genre" << std::endl;
    std::cout << "7. Get average film ratings" << std::endl;
    std::cout << "8. Add new film" << std::endl;
    std::cout << "9. Add new actor" << std::endl;
    std::cout << "10. Update film box office" << std::endl;
    std::cout << "11. Demonstrate all 10 SQL queries" << std::endl;
    std::cout << "12. Film duration statistics (CASE + агрегаты)" << std::endl;  
    std::cout << "13. Exit" << std::endl; 
    std::cout << "Enter your choice (1-13): ";
}
int main() {
    std::cout << "=== Cinema Database Application ===" << std::endl;
    
    // Строка подключения к базе данных
    std::string conn_string = "host=localhost port=5432 dbname=cinema_db "
                             "user=cinema_user password=cinema123";
    
    try {
        CinemaDatabase db(conn_string);
        int choice;
        
        do {
            displayMenu();
            std::cin >> choice;
            std::cin.ignore(); // Очистка буфера
            
            switch(choice) {
                case 1:
                    db.showTestData();
                    break;
                case 2: {
                    int year;
                    std::cout << "Enter year: ";
                    std::cin >> year;
                    db.findFilmsByYear(year);
                    break;
                }
                case 3:
                    db.getDirectorStatistics();
                    break;
                case 4: {
                    std::string title;
                    std::cout << "Enter film title: ";
                    std::getline(std::cin, title);
                    db.findActorsByFilm(title);
                    break;
                }
                case 5: {
                    int limit;
                    std::cout << "Enter limit (default 10): ";
                    std::cin >> limit;
                    db.getTopGrossingFilms(limit);
                    break;
                }
                case 6: {
                    std::string genre;
                    std::cout << "Enter genre: ";
                    std::getline(std::cin, genre);
                    db.findFilmsByGenre(genre);
                    break;
                }
                case 7:
                    db.getAverageFilmRatings();
                    break;
                case 8: {
                    std::string title;
                    int year, duration, director_id;
                    double budget, box_office;
                    
                    std::cout << "Enter title: ";
                    std::getline(std::cin, title);
                    std::cout << "Enter release year: ";
                    std::cin >> year;
                    std::cout << "Enter duration (minutes): ";
                    std::cin >> duration;
                    std::cout << "Enter budget: ";
                    std::cin >> budget;
                    std::cout << "Enter box office: ";
                    std::cin >> box_office;
                    std::cout << "Enter director ID: ";
                    std::cin >> director_id;
                    
                    db.addFilm(title, year, duration, budget, box_office, director_id);
                    break;
                }
                case 9: {
                    std::string first_name, last_name, birth_date, nationality;
                    char oscar;
                    
                    std::cout << "Enter first name: ";
                    std::getline(std::cin, first_name);
                    std::cout << "Enter last name: ";
                    std::getline(std::cin, last_name);
                    std::cout << "Enter birth date (YYYY-MM-DD): ";
                    std::getline(std::cin, birth_date);
                    std::cout << "Enter nationality: ";
                    std::getline(std::cin, nationality);
                    std::cout << "Oscar winner? (y/n): ";
                    std::cin >> oscar;
                    
                    db.addActor(first_name, last_name, birth_date, nationality, 
                               (oscar == 'y' || oscar == 'Y'));
                    break;
                }
                case 10: {
                    int film_id;
                    double box_office;
                    
                    std::cout << "Enter film ID: ";
                    std::cin >> film_id;
                    std::cout << "Enter new box office: ";
                    std::cin >> box_office;
                    
                    db.updateFilmBoxOffice(film_id, box_office);
                    break;
                }
                case 11:
                    db.demonstrateAllQueries();
                    break;
                case 12:
                    db.filmDurationStatistics();
                    break;
                case 13:
                    std::cout << "Goodbye!" << std::endl;
                    break;
                default:
                    std::cout << "Invalid choice!" << std::endl;
            }
        } while (choice != 13);
        
    } catch (const std::exception &e) {
        std::cerr << "Fatal error: " << e.what() << std::endl;
        return 1;
    }
    
    return 0;
}