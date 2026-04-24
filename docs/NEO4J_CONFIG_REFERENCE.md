# Neo4j 配置参考

本文档介绍 Neo4j 适配器的所有配置项及其默认值。

## 基础连接配置

### 连接参数

| 配置项 | 类型 | 默认值 | 说明 |
|-------|------|--------|------|
| `uri` | string | `neo4j://localhost:7687` | Neo4j 服务器连接 URI |
| `username` | string | `neo4j` | 连接用户名 |
| `password` | string | 空字符串 | 连接密码（可选） |
| `database` | string | `neo4j` | 默认数据库名称 |

### 配置方式

#### 1. 使用代码配置

```go
cfg := &db.Config{
    Adapter: "neo4j",
    Neo4j: &db.Neo4jConnectionConfig{
        URI:      "neo4j://custom-host:7687",
        Username: "admin",
        Password: "secret",
        Database: "my_database",
    },
}
repo, err := db.NewRepository(cfg)
```

#### 2. 使用环境变量

```bash
# 基础连接配置
export NEO4J_URI="neo4j://localhost:7687"
export NEO4J_USER="neo4j"          # 或 NEO4J_USERNAME
export NEO4J_PASSWORD="password"
export NEO4J_DATABASE="neo4j"      # 或 NEO4J_DB
```

#### 3. 使用默认值（无需配置）

```go
cfg := &db.Config{
    Adapter: "neo4j",
    Neo4j: &db.Neo4jConnectionConfig{
        // 留空字段将自动使用默认值
    },
}
repo, err := db.NewRepository(cfg)
// 连接到 neo4j://localhost:7687 (用户: neo4j, 密码: 空, 数据库: neo4j)
```

## 社交网络特性配置

### 节点标签配置

| 配置项 | 类型 | 默认值 | 说明 |
|-------|------|--------|------|
| `social_network.user_label` | string | `User` | 用户节点标签 |
| `social_network.chat_room_label` | string | `ChatRoom` | 聊天室节点标签 |
| `social_network.chat_message_label` | string | `ChatMessage` | 消息节点标签 |
| `social_network.post_label` | string | `Post` | 帖子节点标签 |
| `social_network.comment_label` | string | `Comment` | 评论节点标签 |
| `social_network.forum_label` | string | `Forum` | 论坛节点标签 |
| `social_network.emoji_label` | string | `Emoji` | 表情节点标签 |

### 关系类型配置

| 配置项 | 类型 | 默认值 | 说明 |
|-------|------|--------|------|
| `social_network.follows_rel_type` | string | `FOLLOWS` | 关注关系 |
| `social_network.friend_rel_type` | string | `FRIEND` | 好友关系 |
| `social_network.friend_request_rel_type` | string | `FRIEND_REQUEST` | 好友请求关系 |
| `social_network.sent_rel_type` | string | `SENT` | 发送消息关系 |
| `social_network.member_of_rel_type` | string | `MEMBER_OF` | 成员关系 |
| `social_network.in_room_rel_type` | string | `IN` | 用户在聊天室中的关系 |
| `social_network.in_room_msg_rel_type` | string | `IN_ROOM` | 消息所在聊天室关系 |
| `social_network.muted_in_rel_type` | string | `MUTED_IN` | 被禁言关系 |
| `social_network.banned_in_rel_type` | string | `BANNED_IN` | 被封禁关系 |
| `social_network.read_by_rel_type` | string | `READ_BY` | 已读关系 |
| `social_network.authored_rel_type` | string | `AUTHORED` | 作者关系 |
| `social_network.created_rel_type` | string | `CREATED` | 创建关系（聊天室创建者） |

### 全文索引配置

| 配置项 | 类型 | 默认值 | 说明 |
|-------|------|--------|------|
| `social_network.chat_message_fulltext_index` | string | `chat_message_fulltext` | 消息全文索引名称 |

### 策略配置

| 配置项 | 类型 | 默认值 | 可选值 | 说明 |
|-------|------|--------|-------|------|
| `social_network.join_room_strategy` | string | `request_approval` | `request_approval` \| `open` | 加入聊天室策略 |
| `social_network.direct_chat_permission` | string | `mutual_follow_or_friend` | `mutual_follow_or_friend` \| `friends_only` \| `mutual_follow_only` \| `open` | 私信权限策略 |

### 权限配置

| 配置项 | 类型 | 默认值 | 说明 |
|-------|------|--------|------|
| `social_network.moderation_rel_types` | []string | `["CREATED"]` | 具备 mute/ban 权限的关系类型 |
| `social_network.permission_levels` | []string | `["member", "moderator", "admin", "creator"]` | 成员权限级别定义 |

## 配置示例

### 完整的社交网络配置

```yaml
database:
  adapter: neo4j
  neo4j:
    uri: "neo4j://localhost:7687"
    username: "neo4j"
    password: "password123"
    database: "social_network"
    social_network:
      # 节点标签
      user_label: "User"
      chat_room_label: "ChatRoom"
      chat_message_label: "ChatMessage"
      post_label: "Post"
      comment_label: "Comment"
      forum_label: "Forum"
      emoji_label: "Emoji"
      
      # 关系类型
      follows_rel_type: "FOLLOWS"
      friend_rel_type: "FRIEND"
      friend_request_rel_type: "FRIEND_REQUEST"
      sent_rel_type: "SENT"
      member_of_rel_type: "MEMBER_OF"
      in_room_rel_type: "IN"
      in_room_msg_rel_type: "IN_ROOM"
      muted_in_rel_type: "MUTED_IN"
      banned_in_rel_type: "BANNED_IN"
      read_by_rel_type: "READ_BY"
      authored_rel_type: "AUTHORED"
      created_rel_type: "CREATED"
      
      # 索引和策略
      chat_message_fulltext_index: "chat_message_fulltext"
      join_room_strategy: "request_approval"
      direct_chat_permission: "mutual_follow_or_friend"
      
      # 权限配置
      moderation_rel_types:
        - "CREATED"
        - "MODERATOR"
      permission_levels:
        - "member"
        - "moderator"
        - "admin"
        - "creator"
```

### 环境变量配置示例

```bash
# 基础连接
export NEO4J_URI="neo4j://localhost:7687"
export NEO4J_USER="neo4j"
export NEO4J_PASSWORD="password123"
export NEO4J_DATABASE="social_network"

# 社交网络配置
export NEO4J_SOCIAL_USER_LABEL="User"
export NEO4J_SOCIAL_CHAT_ROOM_LABEL="ChatRoom"
export NEO4J_SOCIAL_CHAT_MESSAGE_LABEL="ChatMessage"
export NEO4J_SOCIAL_POST_LABEL="Post"
export NEO4J_SOCIAL_COMMENT_LABEL="Comment"
export NEO4J_SOCIAL_FORUM_LABEL="Forum"
export NEO4J_SOCIAL_EMOJI_LABEL="Emoji"

# 关系类型
export NEO4J_SOCIAL_FOLLOWS_REL="FOLLOWS"
export NEO4J_SOCIAL_FRIEND_REL="FRIEND"
export NEO4J_SOCIAL_FRIEND_REQUEST_REL="FRIEND_REQUEST"
export NEO4J_SOCIAL_SENT_REL="SENT"
export NEO4J_SOCIAL_MEMBER_OF_REL="MEMBER_OF"
export NEO4J_SOCIAL_IN_ROOM_REL="IN"
export NEO4J_SOCIAL_IN_ROOM_MSG_REL="IN_ROOM"
export NEO4J_SOCIAL_MUTED_IN_REL="MUTED_IN"
export NEO4J_SOCIAL_BANNED_IN_REL="BANNED_IN"
export NEO4J_SOCIAL_READ_BY_REL="READ_BY"
export NEO4J_SOCIAL_AUTHORED_REL="AUTHORED"
export NEO4J_SOCIAL_CREATED_REL="CREATED"

# 策略
export NEO4J_SOCIAL_FULLTEXT_INDEX="chat_message_fulltext"
export NEO4J_SOCIAL_JOIN_ROOM_STRATEGY="request_approval"
export NEO4J_SOCIAL_DIRECT_CHAT_PERMISSION="mutual_follow_or_friend"
```

## 快速开始

### 1. 使用全部默认值

如果你使用本地 Neo4j 实例（默认端口 7687），用户名为 "neo4j"，无需任何额外配置：

```go
cfg := &db.Config{
    Adapter: "neo4j",
    Neo4j: &db.Neo4jConnectionConfig{},
}
repo, err := db.NewRepository(cfg)
// 连接成功！
```

### 2. 部分覆盖默认值

```go
cfg := &db.Config{
    Adapter: "neo4j",
    Neo4j: &db.Neo4jConnectionConfig{
        URI: "neo4j://production-server:7687",
        // Username 和 Database 将使用默认值
    },
}
repo, err := db.NewRepository(cfg)
```

### 3. 自定义社交网络标签

```go
cfg := &db.Config{
    Adapter: "neo4j",
    Neo4j: &db.Neo4jConnectionConfig{
        URI:      "neo4j://localhost:7687",
        Username: "neo4j",
        Database: "social",
        SocialNetwork: &db.Neo4jSocialNetworkConfig{
            UserLabel:     "Account",
            ChatRoomLabel: "Room",
            PostLabel:     "Article",
            // 其他字段将使用默认值
        },
    },
}
repo, err := db.NewRepository(cfg)
```

## 常见问题

### Q: 如何在不修改代码的情况下切换 Neo4j 服务器？

**A:** 使用环境变量：

```bash
export NEO4J_URI="neo4j://new-server:7687"
export NEO4J_PASSWORD="new_password"
```

### Q: 定时任务（Scheduled Tasks）在 Neo4j 中是否支持？

**A:** 不支持。Neo4j 没有原生的定时任务功能。系统会自动 fallback 到应用层的 cron 调度器。

### Q: 如何验证我的配置？

**A:** 使用 `Ping()` 方法验证连接：

```go
repo, err := db.NewRepository(cfg)
if err := repo.Ping(context.Background()); err != nil {
    fmt.Printf("连接失败: %v\n", err)
} else {
    fmt.Println("连接成功！")
}
```

### Q: 默认的社交网络标签是否适合我的数据模型？

**A:** 默认标签是通用的，但如果你有特定的命名约定，建议在 `SocialNetwork` 配置中进行自定义。

## 相关文件

- [neo4j_adapter.go](../neo4j_adapter.go) - Neo4j 适配器实现
- [config.go](../config.go) - 配置结构和解析逻辑
- [neo4j_features.go](../neo4j_features.go) - Neo4j 功能支持声明
