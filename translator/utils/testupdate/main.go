package main

import (
	"fmt"

	translator "github.com/cloudspannerecosystem/dynamodb-adapter/translator/utils"
)

func main() {
	transaltorObj := translator.Translator{}
	query := "UPDATE employee SET status = 'active', address = 'new address', age = 31 WHERE emp_id = 'eqi;"
	// query := "SELECT * FROM users WHERE name = 'Alice' OR city = 'New York';"
	// query := "SELECT employee_id, employee_name FROM employees WHERE data -> 'address' -> 'city' = 'New York' AND data -> 'age' > 30;"
	res, err := transaltorObj.ToSpannerUpdate(query)
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println("response-> ", res)
}
