package internal

import (
	"fmt"
	"strings"
)

type NamePath struct {
	path   []string
	maxNum int
}

func (p *NamePath) isInformationSchema(path []string) bool {
	if len(path) == 0 {
		return false
	}
	// If INFORMATION_SCHEMA is at the end of path, ignore it.
	for _, subPath := range path[:len(path)-1] {
		if strings.EqualFold(subPath, "information_schema") {
			return true
		}
	}
	return false
}

func (p *NamePath) setMaxNum(num int) {
	if num > 0 {
		p.maxNum = num
	}
}

func (p *NamePath) getMaxNum(path []string) int {
	if p.maxNum == 0 {
		return 0
	}
	// INFORMATION_SCHEMA is a special View.
	// This means that one path is added to the rules for specifying a normal Table/Function.
	if p.isInformationSchema(path) {
		return p.maxNum + 1
	}
	return p.maxNum
}

func (p *NamePath) normalizePath(path []string) []string {
	ret := []string{}
	for _, p := range path {
		splitted := strings.Split(p, ".")
		ret = append(ret, splitted...)
	}
	return ret
}

func (p *NamePath) mergePath(path []string) []string {
	path = p.normalizePath(path)
	maxNum := p.getMaxNum(path)
	if maxNum > 0 && len(path) == maxNum {
		return path
	}
	if len(path) == 0 {
		return p.path
	}
	merged := []string{}
	for _, basePath := range p.path {
		if path[0] == basePath {
			break
		}
		if maxNum > 0 && len(merged)+len(path) >= maxNum {
			break
		}
		merged = append(merged, basePath)
	}
	return append(merged, path...)
}

func (p *NamePath) format(path []string) string {
	return formatPath(p.mergePath(path))
}

func formatPath(path []string) string {
	return strings.Join(path, "_")
}

func (p *NamePath) setPath(path []string) error {
	normalizedPath := p.normalizePath(path)
	maxNum := p.getMaxNum(path)
	if maxNum > 0 && len(normalizedPath) > maxNum {
		return fmt.Errorf("specified too many name paths %v(%d). max name path is %d", path, len(normalizedPath), maxNum)
	}
	p.path = normalizedPath
	return nil
}

func (p *NamePath) addPath(path string) error {
	normalizedPath := p.normalizePath([]string{path})
	totalPath := len(p.path) + len(normalizedPath)
	maxNum := p.getMaxNum(normalizedPath)
	if maxNum > 0 && totalPath > maxNum {
		return fmt.Errorf(
			"specified too many name paths %v(%d). max name path is %d",
			append(p.path, normalizedPath...),
			totalPath,
			maxNum,
		)
	}
	p.path = append(p.path, normalizedPath...)
	return nil
}

func (p *NamePath) empty() bool {
	return len(p.path) == 0
}
