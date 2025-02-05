package v1

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/cloudspannerecosystem/dynamodb-adapter/models"
	"github.com/cloudspannerecosystem/dynamodb-adapter/service/services"
	"github.com/gin-gonic/gin"
	"github.com/tj/assert"

	"github.com/stretchr/testify/mock"
)

// MockService struct
type MockService struct {
	mock.Mock
}

// Mock TransactGetItems method
func (m *MockService) TransactGetItems(ctx context.Context, getRequest models.GetItemRequest, keyMapArray []map[string]interface{}, projectionExpression string, expressionAttributeNames map[string]string, st services.Storage) ([]map[string]interface{}, error) {
	args := m.Called(ctx, getRequest, keyMapArray, projectionExpression, expressionAttributeNames, st)
	return args.Get(0).([]map[string]interface{}), args.Error(1)
}

func (m *MockService) MayIReadOrWrite(tableName string, isWrite bool, user string) bool {
	args := m.Called(tableName, isWrite, user)
	return args.Bool(0)
}

func (m *MockService) ChangeMaptoDynamoMap(input interface{}) (map[string]interface{}, error) {
	args := m.Called(input)
	return args.Get(0).(map[string]interface{}), args.Error(1)
}

// Test case for TransactGetItems

func TestTransactGetItems_ValidRequestWithMultipleItems(t *testing.T) {
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)

	mockService := new(MockService)

	// Mock MayIReadOrWrite
	mockService.On("MayIReadOrWrite", "employee", false, "").Return(true)

	// Mock TransactGetItems response
	mockService.On("TransactGetItems",
		mock.Anything,
		mock.Anything,
		mock.Anything, // Use mock.Anything for keyMapArray for now
		mock.Anything, // Correctly match the empty string
		mock.Anything,
		mock.Anything,
	).Return([]map[string]interface{}{
		{"emp_id": 1, "first_name": "John", "last_name": "Doe"},
		{"emp_id": 2, "first_name": "Jane", "last_name": "Smith"},
	}, nil).Twice()

	// Create request payload
	transactGetMeta := models.TransactGetItemsRequest{
		TransactItems: []models.TransactGetItem{
			{
				Get: models.GetItemRequest{
					TableName: "employee",
					Keys: map[string]*dynamodb.AttributeValue{
						"emp_id": {N: aws.String("1")},
					},
				},
			},
			{
				Get: models.GetItemRequest{
					TableName: "employee",
					Keys: map[string]*dynamodb.AttributeValue{
						"emp_id": {N: aws.String("2")},
					},
				},
			},
		},
	}
	//models.TableColChangeMap[tableName]
	reqBody, _ := json.Marshal(transactGetMeta)

	// Set request in context
	c.Request, _ = http.NewRequest("POST", "/transact-get-items", bytes.NewBuffer(reqBody))

	// Set mock service in context
	services.SetServiceInstance(mockService)

	// Call the handler function
	TransactGetItems(c, mockService)

	// Assertions
	assert.Equal(t, http.StatusOK, recorder.Code)

	var responseBody models.TransactGetItemsResponse // Use the correct response type
	err := json.Unmarshal(recorder.Body.Bytes(), &responseBody)
	assert.NoError(t, err, "Response should be valid JSON")
	// Access the Responses field
	responses := responseBody.Responses

	// Check that response contains expected employee data
	expectedEmployees := []map[string]interface{}{
		{"emp_id": map[string]interface{}{"N": "1"}, "first_name": map[string]interface{}{"S": "John"}, "last_name": map[string]interface{}{"S": "Doe"}},
		{"emp_id": map[string]interface{}{"N": "2"}, "first_name": map[string]interface{}{"S": "Jane"}, "last_name": map[string]interface{}{"S": "Smith"}},
	}

	assert.Equal(t, len(expectedEmployees), len(responses), "Response should contain correct number of items")
	// for _, response := range responses {
	// 	employeeData, exists := response.Item["employee"].(map[string]interface{})
	// 	assert.True(t, exists, "Response should contain 'employee' key")

	// 	// Access the "L" key
	// 	employeeList, exists := employeeData["L"].(interface{}) // Type assertion tointerface{}
	// 	assert.True(t, exists, "Response should contain 'L' key")

	// 	// Now you can use len() and indexing on employeeList
	// 	assert.Equal(t, 2, len(employeeList), "Employee list should contain two employees")

	// 	for j, expected := range expectedEmployees {
	// 		assert.Equal(t, expected["emp_id"], employeeList[j].(map[string]interface{})["emp_id"])
	// 		assert.Equal(t, expected["first_name"], employeeList[j].(map[string]interface{})["first_name"])
	// 		assert.Equal(t, expected["last_name"], employeeList[j].(map[string]interface{})["last_name"])
	// 	}
	// }

	// Verify that the mock expectations were met
	mockService.AssertExpectations(t)
}
