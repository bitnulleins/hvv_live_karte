// 01 Initialize map
var mymap = L.map('mapid').setView([53.556,10.009], 13);
var osmLayer = L.tileLayer('https://tile.openstreetmap.org/{z}/{x}/{y}.png', {
    attribution: 'Map data &copy; <a href="https://www.openstreetmap.org/">OpenStreetMap</a> contributors, <a href="https://creativecommons.org/licenses/by-sa/2.0/">CC-BY-SA</a>, Imagery © <a href="https://www.mapbox.com/">Mapbox</a>',
    maxZoom: 18,
}).addTo(mymap);

// 02 Markers
mapMarkers = [];
heatLayers = [];

// Listen to event...
var source = new EventSource('/topic/fahrten');
source.addEventListener('message', function(e) {
  // Reset all markers
  mapMarkers.forEach(element => {
    mymap.removeLayer(element);
  });

  // Add new objects to map
  hvv_objects = JSON.parse(e.data);
  hvv_objects.forEach(obj => {
    lat = obj.tracks.end[0]
    lon = obj.tracks.end[1]

    // Custom class names
    className = "vehicleType-"+obj.vehicleType
    className += ` ${obj.realtimeDelay}min-delayed`
    if (obj.realtimeDelay > 5) className += " delayed"

    // Custom icon
    icon = L.icon({
      iconUrl: `https://cloud.geofox.de/icon/linename?name=${obj.line}&outlined=true&fileFormat=SVG&height=16&appearance=COLOURED`,
      className: className,
      iconAnchor:   [8, 8], // point of the icon which will correspond to marker's location
      popupAnchor:  [0, -16] // point from which the popup should open relative to the iconAnchor
    });

    // Popup
    var popup = L.popup({minWidth: 00})
    .setContent(`Von: ${obj.origin}<br />
                Bis: ${obj.destination}<br />
                Linie: ${obj.line}<br />
                Nächster Halt: ${obj.endStationName}<br />
                Delay: ${obj.realtimeDelay}`);

    marker = L.marker([lat, lon], {icon: icon}).bindPopup(popup).addTo(mymap)
    mapMarkers.push(marker)
  });

});


// 03 BATCH TASK: HEATMAP after Marker reload (Kafka reaction)

fetch('/analyze')
    .then(response => {
      if (!response.ok) throw new Error(response.error)
      return response.json()
    })
    .then(values => {
      for (const [id, avg_delay_val] of Object.entries(values)) {
        document.getElementById(id).innerText = avg_delay_val
      }

      fetch('../static/heatmap.json')
      .then(response => response.json())
      .then(values => {
    
        // Clean heat layers
        heatLayers.forEach(element => {
          console.log("Remove heat layers")
          mymap.removeLayer(element);
        });

        heatValues = values.map(value => [value.lat, value.lon, value.avg_delay])
        heatLayer = L.heatLayer(heatValues, {radius:25, max: parseInt(document.getElementById('color-5').innerText.substr(0,4)), gradient: {0.2:'blue',0.4:'cyan',0.6:'lime',0.8:'yellow',0.9:'red'}});

        heatLayer.addTo(mymap);
        heatLayers.push(heatLayer);
        
        // 04 Layer Controls
        var baseMaps = {
          "OpenStreetMap": osmLayer
        };
        var overlayMaps= {
          "Delay Heatmap": heatLayer
        }
    
        // Add controls to UI
        L.control.layers(baseMaps, overlayMaps).addTo(mymap);
    })
  });