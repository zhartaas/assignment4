package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/go-redis/redis/v8"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"log"
)

var dsn string = "user=postgres dbname=assignment2 sslmode=disable password=5641 host=localhost"
var db *sqlx.DB
var rdb *redis.Client
var ctx = context.Background()

type Task struct {
	ID        int    `db:"id"`
	Name      string `db:"name"`
	Completed bool   `db:"completed"`
}

func main() {
	var err error

	db, err = openDB(dsn)
	if err != nil {
		log.Fatal("Error connecting to database:", err)
	}
	defer db.Close()

	rdb = redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	defer rdb.Close()

	insertTask("finish assignment2", false)
	insertTask("finish assignment3", false)
	insertTask("finish assignment2", true)

	fmt.Println("First request:")
	getTaskByID(1)

	fmt.Println("Second request:")
	getTaskByID(1)

	fmt.Println("Third request:")
	getTaskByID(2)
}

func taskCompleted(id int) {
	_, err := db.Exec("UPDATE tasks SET completed = true where id = $1", id)
	if err != nil {
		panic(err)
	}
}

func deleteTask(id int) {
	_, err := db.Exec("DELETE FROM tasks WHERE id = $1", id)
	if err != nil {
		panic(err)
	}
}

func getAllTasks() []*Task {
	tasks := []*Task{}
	rows, err := db.Query("SELECT * FROM tasks")
	if err != nil {
		panic(err)
	}

	for rows.Next() {
		task := &Task{}
		err := rows.Scan(&task.ID, &task.Name, &task.Completed)
		if err != nil {
			panic(err)
		}
		tasks = append(tasks, task)
	}
	return tasks
}

func insertTask(name string, completed bool) {
	stmt := "INSERT INTO tasks (name, completed) VALUES ($1, $2)"

	if checkUnique(name) {
		fmt.Printf("error, %s already in DB\n", name)
		return
	}

	_, err := db.Exec(stmt, name, completed)

	if err != nil {
		panic(err)
		return
	}

}

func checkUnique(name string) bool {
	row := db.QueryRow("SELECT * FROM tasks WHERE name=$1", name)
	task := &Task{}
	err := row.Scan(&task.ID, &task.Name, &task.Completed)
	return err != sql.ErrNoRows
}

func openDB(dsn string) (*sqlx.DB, error) {
	db, err := sqlx.Open("postgres", dsn)

	if err != nil {
		return nil, err
	}

	if err = db.Ping(); err != nil {
		return nil, err
	}

	return db, nil
}
func getTaskByID(id int) *Task {
	cacheKey := fmt.Sprintf("task:%d", id)
	cachedTaskJSON, err := rdb.Get(ctx, cacheKey).Result()
	if err == nil {
		var cachedTask Task
		if err := json.Unmarshal([]byte(cachedTaskJSON), &cachedTask); err != nil {
			log.Println("Error decoding cached task:", err)
		} else {
			fmt.Println("Task retrieved from cache:", cachedTask)
			return &cachedTask
		}
	}

	var task Task
	err = db.Get(&task, "SELECT * FROM tasks WHERE id = $1", id)
	if err != nil {
		log.Println("Error fetching task from database:", err)
		return nil
	}

	taskJSON, err := json.Marshal(task)
	if err != nil {
		log.Println("Error encoding task to JSON:", err)
	} else {
		err := rdb.Set(ctx, cacheKey, taskJSON, 0).Err()
		if err != nil {
			log.Println("Error caching task:", err)
		}
	}

	fmt.Println("Task retrieved from database:", task)
	return &task
}
