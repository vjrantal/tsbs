package kusto

import (
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/devops"
	"github.com/timescale/tsbs/query"
)

// TODO: Remove the need for this by continuing to bubble up errors
func panicIfErr(err error) {
	if err != nil {
		panic(err.Error())
	}
}

// Devops produces Kusto-specific queries for all the devops query types.
type Devops struct {
	*devops.Core
}

// NewDevops makes an Devops object ready to generate Queries.
func NewDevops(start, end time.Time, scale int) *Devops {
	core, err := devops.NewCore(start, end, scale)
	panicIfErr(err)
	return &Devops{core}
}

// GenerateEmptyQuery returns an empty query.HTTP
func (d *Devops) GenerateEmptyQuery() query.Query {
	return query.NewHTTP()
}

func (d *Devops) getHostWhereWithHostnames(hostnames []string) string {
	hostnameClauses := []string{}
	for _, s := range hostnames {
		hostnameClauses = append(hostnameClauses, fmt.Sprintf("'%s'", s))
	}

	combinedHostnameClause := strings.Join(hostnameClauses, ", ")
	return "hostname in (" + combinedHostnameClause + ")"
}

func (d *Devops) getHostWhereString(nHosts int) string {
	hostnames, err := d.GetRandomHosts(nHosts)
	panicIfErr(err)
	return d.getHostWhereWithHostnames(hostnames)
}

func (d *Devops) getSelectClausesAggMetrics(agg string, metrics []string) []string {
	selectClauses := make([]string, len(metrics))
	for i, m := range metrics {
		selectClauses[i] = fmt.Sprintf("%s(%s)", agg, m)
	}

	return selectClauses
}

// GroupByTime selects the MAX for numMetrics metrics under 'cpu',
// per minute for nhosts hosts,
// e.g. in pseudo-SQL:
//
// SELECT minute, max(metric1), ..., max(metricN)
// FROM cpu
// WHERE (hostname = '$HOSTNAME_1' OR ... OR hostname = '$HOSTNAME_N')
// AND time >= '$HOUR_START' AND time < '$HOUR_END'
// GROUP BY minute ORDER BY minute ASC
func (d *Devops) GroupByTime(qi query.Query, nHosts, numMetrics int, timeRange time.Duration) {
	interval := d.Interval.MustRandWindow(timeRange)
	metrics, err := devops.GetCPUMetricsSlice(numMetrics)
	panicIfErr(err)
	selectClauses := d.getSelectClausesAggMetrics("max", metrics)
	whereHosts := d.getHostWhereString(nHosts)

	humanLabel := fmt.Sprintf("Kusto %d cpu metric(s), random %4d hosts, random %s by 1m", numMetrics, nHosts, timeRange)
	humanDesc := fmt.Sprintf("%s: %s", humanLabel, interval.StartString())
	kql := fmt.Sprintf("cpu | where %s and timestamp >= datetime('%s') and timestamp < datetime('%s') | summarize %s by bin(timestamp, 1m) | order by timestamp asc", whereHosts, interval.StartString(), interval.EndString(), strings.Join(selectClauses, ", "))
	d.fillInQuery(qi, humanLabel, humanDesc, kql)
}

// GroupByOrderByLimit benchmarks a query that has a time WHERE clause, that groups by a truncated date, orders by that date, and takes a limit:
// SELECT date_trunc('minute', time) AS t, MAX(cpu) FROM cpu
// WHERE time < '$TIME'
// GROUP BY t ORDER BY t DESC
// LIMIT $LIMIT
func (d *Devops) GroupByOrderByLimit(qi query.Query) {
	interval := d.Interval.MustRandWindow(time.Hour)
	where := fmt.Sprintf("where timestamp < datetime('%s')", interval.EndString())

	humanLabel := "Kusto max cpu over last 5 min-intervals (random end)"
	humanDesc := fmt.Sprintf("%s: %s", humanLabel, interval.StartString())
	kql := fmt.Sprintf(`cpu | %s | summarize max(usage_user) by bin(timestamp, 1m) | top 5 by timestamp desc`, where)
	d.fillInQuery(qi, humanLabel, humanDesc, kql)
}

// GroupByTimeAndPrimaryTag selects the AVG of numMetrics metrics under 'cpu' per device per hour for a day,
// e.g. in pseudo-SQL:
//
// SELECT AVG(metric1), ..., AVG(metricN)
// FROM cpu
// WHERE time >= '$HOUR_START' AND time < '$HOUR_END'
// GROUP BY hour, hostname ORDER BY hour
func (d *Devops) GroupByTimeAndPrimaryTag(qi query.Query, numMetrics int) {
	metrics, err := devops.GetCPUMetricsSlice(numMetrics)
	panicIfErr(err)
	interval := d.Interval.MustRandWindow(devops.DoubleGroupByDuration)
	selectClauses := d.getSelectClausesAggMetrics("avg", metrics)

	humanLabel := devops.GetDoubleGroupByLabel("Kusto", numMetrics)
	humanDesc := fmt.Sprintf("%s: %s", humanLabel, interval.StartString())
	kql := fmt.Sprintf("cpu | where timestamp >= datetime('%s') and timestamp < datetime('%s') | summarize %s by bin(timestamp, 1h), hostname | order by timestamp", interval.StartString(), interval.EndString(), strings.Join(selectClauses, ", "))
	d.fillInQuery(qi, humanLabel, humanDesc, kql)
}

// MaxAllCPU selects the MAX of all metrics under 'cpu' per hour for nhosts hosts,
// e.g. in pseudo-SQL:
//
// SELECT MAX(metric1), ..., MAX(metricN)
// FROM cpu WHERE (hostname = '$HOSTNAME_1' OR ... OR hostname = '$HOSTNAME_N')
// AND time >= '$HOUR_START' AND time < '$HOUR_END'
// GROUP BY hour ORDER BY hour
func (d *Devops) MaxAllCPU(qi query.Query, nHosts int) {
	interval := d.Interval.MustRandWindow(devops.MaxAllDuration)
	whereHosts := d.getHostWhereString(nHosts)
	selectClauses := d.getSelectClausesAggMetrics("max", devops.GetAllCPUMetrics())

	humanLabel := devops.GetMaxAllLabel("Kusto", nHosts)
	humanDesc := fmt.Sprintf("%s: %s", humanLabel, interval.StartString())
	kql := fmt.Sprintf("cpu | where %s and timestamp >= datetime('%s') and timestamp < datetime('%s') | summarize %s by bin(timestamp, 1h) | order by timestamp", whereHosts, interval.StartString(), interval.EndString(), strings.Join(selectClauses, ","))
	d.fillInQuery(qi, humanLabel, humanDesc, kql)
}

// LastPointPerHost finds the last row for every host in the dataset
func (d *Devops) LastPointPerHost(qi query.Query) {
	humanLabel := "Kusto last row per host"
	humanDesc := humanLabel + ": cpu"
	kql := "cpu | summarize arg_max(timestamp, *) by hostname"
	d.fillInQuery(qi, humanLabel, humanDesc, kql)
}

// HighCPUForHosts populates a query that gets CPU metrics when the CPU has high
// usage between a time period for a number of hosts (if 0, it will search all hosts),
// e.g. in pseudo-SQL:
//
// SELECT * FROM cpu
// WHERE usage_user > 90.0
// AND time >= '$TIME_START' AND time < '$TIME_END'
// AND (hostname = '$HOST' OR hostname = '$HOST2'...)
func (d *Devops) HighCPUForHosts(qi query.Query, nHosts int) {
	interval := d.Interval.MustRandWindow(devops.HighCPUDuration)

	var hostWhereClause string
	if nHosts == 0 {
		hostWhereClause = ""
	} else {
		hostWhereClause = fmt.Sprintf("and %s", d.getHostWhereString(nHosts))
	}

	humanLabel, err := devops.GetHighCPULabel("Kusto", nHosts)
	panicIfErr(err)
	humanDesc := fmt.Sprintf("%s: %s", humanLabel, interval.StartString())
	kql := fmt.Sprintf("cpu | where usage_user > 90.0 %s and timestamp >= datetime('%s') and timestamp < datetime('%s')", hostWhereClause, interval.StartString(), interval.EndString())
	d.fillInQuery(qi, humanLabel, humanDesc, kql)
}

func (d *Devops) fillInQuery(qi query.Query, humanLabel, humanDesc, kql string) {
	v := url.Values{}
	kql = "let cpu = CpuMetrics_v2; " + kql
	v.Set("csl", kql)
	q := qi.(*query.HTTP)
	q.HumanLabel = []byte(humanLabel)
	q.HumanDescription = []byte(humanDesc)
	q.Method = []byte("GET")
	q.Path = []byte(fmt.Sprintf("/v2/rest/query?%s", v.Encode()))
	q.Body = nil
}
