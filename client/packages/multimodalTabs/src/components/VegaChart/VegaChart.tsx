import { useEffect, useRef } from "react";
import embed from "vega-embed";
import { expressionInterpreter } from "vega-interpreter";

interface VegaChartProps {
  spec: object;
}

export function VegaChart({ spec }: VegaChartProps) {
  const containerRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    const embedChart = async () => {
      if (!containerRef.current || !spec) return;

      try {
        await embed(containerRef.current, spec, {
          actions: {
            export: true,
            source: false,
            compiled: false,
            editor: false,
          },
          renderer: "svg",
          ast: true,
          expr: expressionInterpreter,
        });
      } catch (error) {
        console.error("Failed to render Vega chart:", error);
      }
    };

    embedChart();

    const container = containerRef.current;

    return () => {
      if (container) {
        container.innerHTML = "";
      }
    };
  }, [spec]);

  return (
    <div ref={containerRef} style={{ width: "100%", minHeight: "300px" }} />
  );
}
