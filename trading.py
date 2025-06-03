//@version=5
indicator("Intra-Candle Time Levels (Blue & Yellow Dots)", overlay=true)

// Define timeframes
tf_12min = "12"
tf_14min = "14"

// tf_12min = "12"
t_12min_close = request.security(syminfo.tickerid, tf_12min, close)
t_14min_close = request.security(syminfo.tickerid, tf_14min, close)

// Optional: Plot horizontal line at 12-min close
// plot(t_12min_close, color=color.red, linewidth=1, title="12-min Close Line")

// Shift dot slightly upward to avoid overlap
plotshape(t_12min_close + 0.1, title="12-min Close Dot", location=location.absolute, style=shape.circle, size=size.tiny, color=color.blue)

// Plot yellow dot for 14-min close
// plotshape(t_14min_close, title="14-min Close Dot", location=location.absolute, style=shape.circle, size=size.tiny, color=color.yellow)

#------------------------------------------------------------------------------------
//@version=5
indicator("NATGASMINI - Highlight 6PM to 11PM", overlay=true)

// Get current hour and minute in exchange timezone
t = time("1", "0600-1100")  // 6PM to 11PM

// Plot background highlight
bgcolor(t ? color.new(color.blue, 85) : na)

  
#---
#----



