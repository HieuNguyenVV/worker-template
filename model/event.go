package model

type (
	Event struct {
		Msg      string `json:"msg"`
		CreateAt int64  `json:"create_at"`
	}
)
