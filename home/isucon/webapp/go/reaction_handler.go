package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/labstack/echo-contrib/session"
	"github.com/labstack/echo/v4"
)

type ReactionModel struct {
	ID           int64  `db:"id"`
	EmojiName    string `db:"emoji_name"`
	UserID       int64  `db:"user_id"`
	LivestreamID int64  `db:"livestream_id"`
	CreatedAt    int64  `db:"created_at"`
}

type Reaction struct {
	ID         int64      `json:"id"`
	EmojiName  string     `json:"emoji_name"`
	User       User       `json:"user"`
	Livestream Livestream `json:"livestream"`
	CreatedAt  int64      `json:"created_at"`
}

type PostReactionRequest struct {
	EmojiName string `json:"emoji_name"`
}

func getReactionsHandler(c echo.Context) error {
	ctx := c.Request().Context()

	if err := verifyUserSession(c); err != nil {
		// echo.NewHTTPErrorが返っているのでそのまま出力
		return err
	}

	livestreamID, err := strconv.Atoi(c.Param("livestream_id"))
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "livestream_id in path must be integer")
	}

	tx, err := dbConn.BeginTxx(ctx, nil)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to begin transaction: "+err.Error())
	}
	defer tx.Rollback()

	query := "SELECT * FROM reactions WHERE livestream_id = ? ORDER BY created_at DESC"
	if c.QueryParam("limit") != "" {
		limit, err := strconv.Atoi(c.QueryParam("limit"))
		if err != nil {
			return echo.NewHTTPError(http.StatusBadRequest, "limit query parameter must be integer")
		}
		query += fmt.Sprintf(" LIMIT %d", limit)
	}

	reactionModels := []ReactionModel{}
	if err := tx.SelectContext(ctx, &reactionModels, query, livestreamID); err != nil {
		return echo.NewHTTPError(http.StatusNotFound, "failed to get reactions")
	}

	reactions, err := fillReactionResponseBulk(ctx, tx, reactionModels)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to fill reactions: "+err.Error())
	}	

	if err := tx.Commit(); err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to commit: "+err.Error())
	}

	return c.JSON(http.StatusOK, reactions)
}

func postReactionHandler(c echo.Context) error {
	ctx := c.Request().Context()
	livestreamID, err := strconv.Atoi(c.Param("livestream_id"))
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "livestream_id in path must be integer")
	}

	if err := verifyUserSession(c); err != nil {
		// echo.NewHTTPErrorが返っているのでそのまま出力
		return err
	}

	// error already checked
	sess, _ := session.Get(defaultSessionIDKey, c)
	// existence already checked
	userID := sess.Values[defaultUserIDKey].(int64)

	var req *PostReactionRequest
	if err := json.NewDecoder(c.Request().Body).Decode(&req); err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "failed to decode the request body as json")
	}

	tx, err := dbConn.BeginTxx(ctx, nil)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to begin transaction: "+err.Error())
	}
	defer tx.Rollback()

	reactionModel := ReactionModel{
		UserID:       int64(userID),
		LivestreamID: int64(livestreamID),
		EmojiName:    req.EmojiName,
		CreatedAt:    time.Now().Unix(),
	}

	result, err := tx.NamedExecContext(ctx, "INSERT INTO reactions (user_id, livestream_id, emoji_name, created_at) VALUES (:user_id, :livestream_id, :emoji_name, :created_at)", reactionModel)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to insert reaction: "+err.Error())
	}

	reactionID, err := result.LastInsertId()
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to get last inserted reaction id: "+err.Error())
	}
	reactionModel.ID = reactionID

	reaction, err := fillReactionResponse(ctx, tx, reactionModel)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to fill reaction: "+err.Error())
	}

	if err := tx.Commit(); err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to commit: "+err.Error())
	}

	return c.JSON(http.StatusCreated, reaction)
}

func fillReactionResponse(ctx context.Context, tx *sqlx.Tx, reactionModel ReactionModel) (Reaction, error) {
	userModel := UserModel{}
	if err := tx.GetContext(ctx, &userModel, "SELECT * FROM users WHERE id = ?", reactionModel.UserID); err != nil {
		return Reaction{}, err
	}
	user, err := fillUserResponse(ctx, tx, userModel)
	if err != nil {
		return Reaction{}, err
	}

	livestreamModel := LivestreamModel{}
	if err := tx.GetContext(ctx, &livestreamModel, "SELECT * FROM livestreams WHERE id = ?", reactionModel.LivestreamID); err != nil {
		return Reaction{}, err
	}
	livestream, err := fillLivestreamResponse(ctx, tx, livestreamModel)
	if err != nil {
		return Reaction{}, err
	}

	reaction := Reaction{
		ID:         reactionModel.ID,
		EmojiName:  reactionModel.EmojiName,
		User:       user,
		Livestream: livestream,
		CreatedAt:  reactionModel.CreatedAt,
	}

	return reaction, nil
}

func fillReactionResponseBulk(ctx context.Context, tx *sqlx.Tx, reactionModels []ReactionModel) ([]Reaction, error) {
	// 一応何も無いとき対応
	if len(reactionModels) == 0 {
		return nil, nil
	}

	// 1. UserIDとLivestreamIDを収集
	userIDs := make([]int64, 0, len(reactionModels))
	livestreamIDs := make([]int64, 0, len(reactionModels))
	for _, reaction := range reactionModels {
		userIDs = append(userIDs, reaction.UserID)
		livestreamIDs = append(livestreamIDs, reaction.LivestreamID)
	}

	// 2. ユーザー情報をバルク取得
	var userModels []UserModel
	query, args, err := sqlx.In("SELECT * FROM users WHERE id IN (?)", userIDs)
	if err != nil {
		return nil, fmt.Errorf("failed to build user query: %w", err)
	}
	query = tx.Rebind(query)
	if err := tx.SelectContext(ctx, &userModels, query, args...); err != nil {
		return nil, fmt.Errorf("failed to fetch users: %w", err)
	}
	userMap, err := fillUserResponseBulk(ctx, tx, userModels)
	if err != nil {
		return nil, fmt.Errorf("failed to process user responses: %w", err)
	}

	// 3. ライブストリーム情報をバルク取得
	var livestreamModels []LivestreamModel
	query, args, err = sqlx.In("SELECT * FROM livestreams WHERE id IN (?)", livestreamIDs)
	if err != nil {
		return nil, fmt.Errorf("failed to build livestream query: %w", err)
	}
	query = tx.Rebind(query)
	if err := tx.SelectContext(ctx, &livestreamModels, query, args...); err != nil {
		return nil, fmt.Errorf("failed to fetch livestreams: %w", err)
	}
	livestreamMap, err := fillLivestreamResponseBulk(ctx, tx, livestreamModels)
	if err != nil {
		return nil, fmt.Errorf("failed to process livestream responses: %w", err)
	}

	// 4. Reactionの組み立て
	reactions := make([]Reaction, 0, len(reactionModels))
	for _, reactionModel := range reactionModels {
		// User情報を取得
		user, ok := userMap[reactionModel.UserID]
		if !ok {
			return nil, fmt.Errorf("user not found for ID %d", reactionModel.UserID)
		}

		// Livestream情報を取得
		livestream, ok := livestreamMap[reactionModel.LivestreamID]
		if !ok {
			return nil, fmt.Errorf("livestream not found for ID %d", reactionModel.LivestreamID)
		}

		// Reactionを作成
		reactions = append(reactions, Reaction{
			ID:         reactionModel.ID,
			EmojiName:  reactionModel.EmojiName,
			User:       user,
			Livestream: livestream,
			CreatedAt:  reactionModel.CreatedAt,
		})
	}

	return reactions, nil
}
