//go:build !linux

/*
   Copyright 2022 GitHub Inc.
	 See https://github.com/github/gh-ost/blob/master/LICENSE
*/

package metrics

func newProcessEmitter(_ *Client) func() { return func() {} }
