package tools

import (
	"testing"
)

func TestGenerateOrderedString(t *testing.T) {
	s1 := GenerateOrderedString(1, 10)
	s2 := GenerateOrderedString(2, 10)

	if s1 > s2 {
		t.Errorf("%s should be less than %s", s1, s2)
	}

	s1 = GenerateOrderedString(1, 100000)
	s2 = GenerateOrderedString(100000, 100000)

	if s1 > s2 {
		t.Errorf("%s should be less than %s", s1, s2)
	}
}

func TestGenerateFirstPrimaryKeyColumn(t *testing.T) {
	length := 15
	s1 := GenerateFirstPrimaryKeyColumn(true, true, 1, 64, 1, 1000, length)
	s2 := GenerateFirstPrimaryKeyColumn(true, true, 2, 64, 1, 1000, length)

	if s1 > s2 {
		t.Errorf("%s should be less than %s", s1, s2)
	}

	if len(s1) != length {
		t.Errorf("%s should be less %v characters long when sorted and partitioned are true", s1, length)
	}

	s1 = GenerateFirstPrimaryKeyColumn(true, false, 1, 64, 1, 1000, length)

	if len(s1) != length {
		t.Errorf("%s should be less %v characters long when sorted is true and partitioned is false", s1, length)
	}

	s1 = GenerateFirstPrimaryKeyColumn(false, true, 1, 64, 1, 1000, length)

	if len(s1) != length {
		t.Errorf("%s should be less %v characters long when sorted is false and partitioned is true", s1, length)
	}

	s1 = GenerateFirstPrimaryKeyColumn(false, false, 1, 64, 1, 1000, length)

	if len(s1) != length {
		t.Errorf("%s should be less %v characters long when sorted and partitioned are false", s1, length)
	}

	s1 = GenerateFirstPrimaryKeyColumn(true, true, 1, 64, 1, 1000, length)
	s2 = GenerateFirstPrimaryKeyColumn(true, true, 1, 64, 10, 1000, length)

	if s1 > s2 {
		t.Errorf("%s should be less than %s", s1, s2)
	}

	// Handle a billion records 1 000 000 000
	billion := 1000000000
	s1 = GenerateFirstPrimaryKeyColumn(true, true, 1, 64, billion, billion, length)

	if len(s1) != length {
		t.Errorf("%s should be less %v characters long when creating a billion rows", s1, length)
	}

	// Handle a trillion records 1 000 000 000
	trillion := 1000000000000
	s1 = GenerateFirstPrimaryKeyColumn(true, true, 1, 64, trillion-1, trillion, length)
	s2 = GenerateFirstPrimaryKeyColumn(true, true, 1, 64, trillion, trillion, length)

	if len(s1) != length {
		t.Errorf("%s should be less %v characters long when creating a trillion rows", s1, length)
	}

	if s1 > s2 {
		t.Errorf("%s should be less than %s", s1, s2)
	}

	// First 3 letters should be the same across files if we are sorting but not partitioning
	s1 = GenerateFirstPrimaryKeyColumn(true, false, 1, 64, 1, 1000, length)
	s2 = GenerateFirstPrimaryKeyColumn(true, false, 2, 64, 1, 1000, length)

	if s1[0:3] != s2[0:3] {
		t.Errorf("%s should be the same as %s", s1[0:3], s2[0:3])
	}

	s3 := GenerateFirstPrimaryKeyColumn(true, false, 2, 64, 2, 1000, length)

	if s3 < s2 {
		t.Errorf("%s should be less than %s", s2, s3)
	}

}
