// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package models

//SlotMapping.Action.State
const (
	ActionNothing   = ""             //刚初始化的空slot
	ActionPending   = "pending"      //需要分配了
	ActionPreparing = "preparing"    //准备中
	ActionPrepared  = "prepared"     //准备就绪
	ActionMigrating = "migrating"    //迁移中
	ActionFinished  = "finished"     //迁移完成
	ActionSyncing   = "syncing"      //主从同步中
)
