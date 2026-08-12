"""Human-readable research summary rendering."""

from __future__ import annotations

from typing import Sequence

from app.models import CompanyMetric

from .risk import RiskAnalysis


class ResearchSummaryGenerator:
    """Render deterministic Markdown from stored analysis inputs."""

    def generate(
        self,
        analysis: RiskAnalysis,
        metrics: Sequence[CompanyMetric],
    ) -> str:
        latest_metrics: dict[str, CompanyMetric] = {}
        for metric in sorted(metrics, key=lambda item: item.metric_date):
            latest_metrics[metric.name] = metric

        lines = [
            f"## {analysis.symbol} 每日研究摘要",
            "",
            f"- 資料期間：{analysis.period_start} 至 {analysis.period_end}",
            f"- 有效行情筆數：{analysis.observations}",
            f"- 期初／期末收盤：{analysis.first_close:.2f}／{analysis.last_close:.2f}",
            f"- 區間報酬：{analysis.total_return_pct:.2f}%",
            f"- 平均日報酬：{analysis.average_daily_return_pct:.2f}%",
            f"- 年化歷史波動度：{analysis.annualized_volatility_pct:.2f}%",
            f"- 區間最大回撤：{analysis.max_drawdown_pct:.2f}%",
            f"- 最低／最高收盤：{analysis.minimum_close:.2f}／{analysis.maximum_close:.2f}",
            f"- 平均成交量：{analysis.average_volume:,.0f}",
            "",
            "### 公司指標",
            "",
        ]
        if latest_metrics:
            for name, metric in sorted(latest_metrics.items()):
                unit = f" {metric.unit}" if metric.unit else ""
                lines.append(
                    f"- {name}：{metric.value:.2f}{unit}（{metric.metric_date}）"
                )
        else:
            lines.append("- 本期沒有公司指標資料。")

        lines.extend(
            [
                "",
                "### 使用限制",
                "",
                "- 本摘要只描述已儲存的歷史資料，不產生買賣評分或交易訊號。",
                "- Mock provider 的數值全部是合成測試資料，不可用於真實投資判斷。",
                "- 本內容僅供研究流程測試，不構成投資建議。",
            ]
        )
        return "\n".join(lines)
