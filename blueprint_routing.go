package db

import (
	"fmt"
	"hash/fnv"
	"sort"
	"strings"
)

const (
	// SchedulerPolicyHRWV1 是默认 HRW 稳定路由策略标识。
	SchedulerPolicyHRWV1 = "hrw_v1"
)

// HRWSelectNode 使用 Rendezvous Hash (HRW) 从候选节点 ID 列表中稳定选主。
//
// 选路键由四段组成（顺序固定）：
//
//	requestKey | blueprintTick | routeTick | schedulerPolicy
//
// 每个候选节点的得分 = FNV-64a(routeKey + "|" + candidateNodeID)。
// 得分最高的节点胜出，同输入保证同输出（稳定路由）。
func HRWSelectNode(requestKey, blueprintTick, routeTick, schedulerPolicy string, candidates []string) (string, error) {
	if len(candidates) == 0 {
		return "", fmt.Errorf("hrw route: no candidate nodes")
	}
	if schedulerPolicy == "" {
		schedulerPolicy = SchedulerPolicyHRWV1
	}
	routeKey := buildHRWRouteKey(requestKey, blueprintTick, routeTick, schedulerPolicy)

	var best string
	var bestScore uint64
	for _, candidate := range candidates {
		candidate = strings.TrimSpace(candidate)
		if candidate == "" {
			continue
		}
		score := hrwScore(routeKey, candidate)
		if best == "" || score > bestScore {
			best = candidate
			bestScore = score
		}
	}
	if best == "" {
		return "", fmt.Errorf("hrw route: all candidate node ids were empty")
	}
	return best, nil
}

// HRWRankedCandidates 返回候选节点 ID 按 HRW 得分降序排列的完整列表。
//
// 用于实现节点故障后的降级接管：主节点离线时取列表中下一个在线节点。
func HRWRankedCandidates(requestKey, blueprintTick, routeTick, schedulerPolicy string, candidates []string) ([]string, error) {
	if len(candidates) == 0 {
		return nil, fmt.Errorf("hrw rank: no candidate nodes")
	}
	if schedulerPolicy == "" {
		schedulerPolicy = SchedulerPolicyHRWV1
	}
	routeKey := buildHRWRouteKey(requestKey, blueprintTick, routeTick, schedulerPolicy)

	type scored struct {
		node  string
		score uint64
	}
	scores := make([]scored, 0, len(candidates))
	for _, candidate := range candidates {
		candidate = strings.TrimSpace(candidate)
		if candidate == "" {
			continue
		}
		scores = append(scores, scored{node: candidate, score: hrwScore(routeKey, candidate)})
	}
	if len(scores) == 0 {
		return nil, fmt.Errorf("hrw rank: all candidate node ids were empty")
	}
	sort.Slice(scores, func(i, j int) bool {
		return scores[i].score > scores[j].score
	})
	result := make([]string, len(scores))
	for i, s := range scores {
		result[i] = s.node
	}
	return result, nil
}

// HRWRouteToOnlineNode 在节点列表中按 HRW 稳定选路，仅考虑 status="online" 的节点。
//
// 若无在线节点，返回错误。blueprintTick 与 routeTick 固定时，同 requestKey 路由结果不变。
func HRWRouteToOnlineNode(requestKey, blueprintTick, routeTick string, nodes []CollaborationAdapterNodePresence) (string, error) {
	online := make([]string, 0, len(nodes))
	for _, n := range nodes {
		status := strings.ToLower(strings.TrimSpace(n.Status))
		if status == "online" || status == "" {
			id := strings.TrimSpace(n.NodeID)
			if id != "" {
				online = append(online, id)
			}
		}
	}
	if len(online) == 0 {
		return "", fmt.Errorf("hrw route: no online nodes available")
	}
	return HRWSelectNode(requestKey, blueprintTick, routeTick, SchedulerPolicyHRWV1, online)
}

// HRWRouteToOnlineNodeWithFallback 在主节点离线时沿 HRW 排名降级到下一个在线节点。
//
// preferredNodeID 为当前首选节点（可为空）。若首选节点不在线，则从 HRW 排名中顺序取第一个在线节点。
func HRWRouteToOnlineNodeWithFallback(requestKey, blueprintTick, routeTick string, nodes []CollaborationAdapterNodePresence) (selected string, ranked []string, err error) {
	onlineSet := make(map[string]bool, len(nodes))
	allIDs := make([]string, 0, len(nodes))
	for _, n := range nodes {
		id := strings.TrimSpace(n.NodeID)
		if id == "" {
			continue
		}
		allIDs = append(allIDs, id)
		status := strings.ToLower(strings.TrimSpace(n.Status))
		if status == "online" || status == "" {
			onlineSet[id] = true
		}
	}
	if len(allIDs) == 0 {
		return "", nil, fmt.Errorf("hrw route: no nodes provided")
	}

	ranked, err = HRWRankedCandidates(requestKey, blueprintTick, routeTick, SchedulerPolicyHRWV1, allIDs)
	if err != nil {
		return "", nil, err
	}

	for _, nodeID := range ranked {
		if onlineSet[nodeID] {
			return nodeID, ranked, nil
		}
	}
	return "", ranked, fmt.Errorf("hrw route: no online nodes available after fallback")
}

func buildHRWRouteKey(requestKey, blueprintTick, routeTick, schedulerPolicy string) string {
	return strings.Join([]string{
		strings.TrimSpace(requestKey),
		strings.TrimSpace(blueprintTick),
		strings.TrimSpace(routeTick),
		strings.TrimSpace(schedulerPolicy),
	}, "|")
}

func hrwScore(routeKey, candidate string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(routeKey + "|" + candidate))
	return h.Sum64()
}
